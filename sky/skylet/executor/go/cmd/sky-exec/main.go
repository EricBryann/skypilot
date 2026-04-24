// sky-exec runs on the cluster head node. It is spawned by the skylet
// (services.py) for each job and is responsible for:
//   1. Marking the job RUNNING in the SQLite jobs DB.
//   2. Dialling each node's sky-agent via gRPC and sending the per-node script.
//   3. Streaming log output (with a per-node colored prefix) to run.log and
//      per-node log files.
//   4. Marking the job SUCCEEDED or FAILED when all nodes finish.
//
// Config is read from stdin as JSON (see Config struct).
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "skypilot.dev/executor/gen/agent"
	pbskylet "skypilot.dev/executor/gen/skylet"
	"skypilot.dev/executor/pkg/db"
)

// Config is written by services.py and piped to sky-exec via stdin.
// db_path and lock_dir are intentionally omitted — sky-exec derives them from
// $SKY_RUNTIME_DIR (or $HOME) so the API server never needs to expand paths.
type Config struct {
	JobID        int      `json:"job_id"`
	NodeIPs      []string `json:"node_ips"`
	NodeScripts  []string `json:"node_scripts"`
	NodeLogPaths []string `json:"node_log_paths"`
	LogDir       string   `json:"log_dir"`
	AgentPort    int      `json:"agent_port"`
}

// runtimeDir returns $SKY_RUNTIME_DIR if set, otherwise $HOME.
// Matches sky/skylet/runtime_utils.py:get_runtime_dir_path logic.
func runtimeDir() string {
	if d := os.Getenv("SKY_RUNTIME_DIR"); d != "" {
		return d
	}
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("sky-exec: cannot determine home dir: %v", err)
	}
	return home
}

// ANSI colors — match RayCodeGen output style.
const (
	cyan  = "\033[36m"
	reset = "\033[0m"
)

func nodePrefix(rank int, ip string, pid int32) string {
	if rank == 0 {
		return fmt.Sprintf("%s(head, rank=0, pid=%d)%s ", cyan, pid, reset)
	}
	return fmt.Sprintf("%s(worker%d, rank=%d, ip=%s, pid=%d)%s ", cyan, rank, rank, ip, pid, reset)
}

// scheduleStep calls the skylet's ScheduleStep RPC on localhost to immediately
// pick up the next pending job after this one finishes.
func scheduleStep() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "localhost:46590",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Printf("sky-exec: schedule_step dial: %v", err)
		return
	}
	defer conn.Close()
	client := pbskylet.NewJobsServiceClient(conn)
	if _, err := client.ScheduleStep(ctx, &pbskylet.ScheduleStepRequest{}); err != nil {
		log.Printf("sky-exec: schedule_step RPC: %v", err)
	}
}

func main() {
	var cfg Config
	if err := json.NewDecoder(os.Stdin).Decode(&cfg); err != nil {
		log.Fatalf("sky-exec: decode config: %v", err)
	}

	if cfg.AgentPort == 0 {
		cfg.AgentPort = 50052
	}

	// Expand ~ in paths — Go does not expand tilde automatically.
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("sky-exec: get home dir: %v", err)
	}
	expandHome := func(p string) string {
		if len(p) >= 2 && p[:2] == "~/" {
			return filepath.Join(home, p[2:])
		}
		return p
	}
	cfg.LogDir = expandHome(cfg.LogDir)
	for i, p := range cfg.NodeLogPaths {
		cfg.NodeLogPaths[i] = expandHome(p)
	}

	// Handle SIGTERM / SIGINT: cancel all in-flight gRPC streams.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		cancel()
	}()

	rtDir := runtimeDir()
	dbPath := filepath.Join(rtDir, ".sky", "jobs.db")
	lockDir := filepath.Join(rtDir, ".sky", "locks")

	database, err := db.Open(dbPath)
	if err != nil {
		log.Fatalf("sky-exec: open db: %v", err)
	}
	defer database.Close()

	if err := db.SetRunning(database, lockDir, cfg.JobID); err != nil {
		log.Fatalf("sky-exec: set RUNNING: %v", err)
	}

	// Create the tasks log dir for per-node log files.
	if err := os.MkdirAll(cfg.LogDir, 0755); err != nil {
		log.Fatalf("sky-exec: mkdir log dir: %v", err)
	}

	// Write colored output to stdout — _exec_code_on_head redirects
	// sky-exec's stdout to remote_log_dir/run.log, which is the path
	// tail_logs watches. Per-node logs are still tee'd to NodeLogPaths.
	runLog := os.Stdout

	// Print sentinel messages matching task_codegen._add_waiting_for_resources_msg
	// and _add_job_started_msg exactly. tail_logs_iter blocks until it sees
	// LOG_FILE_START_STREAMING_AT = 'Waiting for task resources on '.
	dim := "\033[2m"
	n := len(cfg.NodeIPs)
	plural := ""
	if n > 1 {
		plural = "s"
	}
	fmt.Fprintf(runLog, "\033[2m├── \033[0m%sWaiting for task resources on %d node%s.\033[0m\n",
		dim, n, plural)
	fmt.Fprintf(runLog, "\033[2m└── \033[0mJob started. Streaming logs... %s(Ctrl-C to exit log streaming; job will not be killed)\033[0m\n",
		dim)

	// mu guards writes to runLog so lines from different nodes don't interleave.
	var mu sync.Mutex

	exitCodes := make([]int, len(cfg.NodeIPs))
	var wg sync.WaitGroup

	for i, ip := range cfg.NodeIPs {
		wg.Add(1)
		go func(rank int, ip string) {
			defer wg.Done()
			exitCodes[rank] = runOnNode(ctx, cfg, ip, rank, runLog, &mu)
		}(i, ip)
	}
	wg.Wait()

	succeeded := true
	for _, code := range exitCodes {
		if code != 0 {
			succeeded = false
			break
		}
	}

	fmt.Fprintf(runLog, "DEBUG sky-exec: calling SetFinal at %s\n", time.Now().Format(time.RFC3339Nano))
	if err := db.SetFinal(database, lockDir, cfg.JobID, succeeded); err != nil {
		log.Printf("sky-exec: set final status: %v", err)
	}
	fmt.Fprintf(runLog, "DEBUG sky-exec: SetFinal done at %s\n", time.Now().Format(time.RFC3339Nano))

	if err := db.SetExitCodes(database, lockDir, cfg.JobID, exitCodes); err != nil {
		log.Printf("sky-exec: set exit codes: %v", err)
	}

	go scheduleStep()

	if !succeeded {
		reason := ""
		has137 := false
		non137 := 0
		for _, code := range exitCodes {
			if code == 139 {
				reason = "(likely due to Segmentation Fault)"
				break
			}
			if code == 137 {
				has137 = true
			} else if code != 0 {
				non137 = code
			}
		}
		if reason == "" && has137 && non137 != 0 {
			reason = fmt.Sprintf("(A Worker failed with return code %d, SkyPilot cleaned up the processes on other nodes with return code 137)", non137)
		}
		red := "\033[31m"
		fmt.Fprintf(runLog, "ERROR: %sJob %d failed with return code list:%s %v %s\n",
			red, cfg.JobID, reset, exitCodes, reason)
		os.Exit(1)
	}
}

// runOnNode dials the sky-agent on ip, sends the script, streams log lines,
// and returns the script's exit code.
func runOnNode(ctx context.Context, cfg Config, ip string, rank int, runLog io.Writer, mu *sync.Mutex) int {
	addr := fmt.Sprintf("%s:%d", ip, cfg.AgentPort)
	dialCtx, dialCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dialCancel()
	conn, err := grpc.DialContext(dialCtx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		writeLine(mu, runLog, nodePrefix(rank, ip, 0), fmt.Sprintf(
			"ERROR: connect to %s (is sky-agent running?): %v", addr, err))
		return 1
	}
	defer conn.Close()

	// Open per-node log file.
	nodeLogPath := cfg.NodeLogPaths[rank]
	if err := os.MkdirAll(filepath.Dir(nodeLogPath), 0755); err != nil {
		writeLine(mu, runLog, nodePrefix(rank, ip, 0), fmt.Sprintf("ERROR: mkdir node log dir: %v", err))
		return 1
	}
	nodeLog, err := os.OpenFile(nodeLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		writeLine(mu, runLog, nodePrefix(rank, ip, 0), fmt.Sprintf("ERROR: open node log: %v", err))
		return 1
	}
	defer nodeLog.Close()

	client := pb.NewAgentServiceClient(conn)
	stream, err := client.Execute(ctx, &pb.ExecuteRequest{
		Script:   cfg.NodeScripts[rank],
		JobId:    int32(cfg.JobID),
		NodeRank: int32(rank),
	})
	if err != nil {
		writeLine(mu, runLog, nodePrefix(rank, ip, 0), fmt.Sprintf("ERROR: Execute RPC: %v", err))
		return 1
	}

	// First response carries the PID; no output bytes.
	firstResp, err := stream.Recv()
	if err != nil {
		writeLine(mu, runLog, nodePrefix(rank, ip, 0), fmt.Sprintf("ERROR: recv pid: %v", err))
		return 1
	}
	prefix := nodePrefix(rank, ip, firstResp.Pid)
	var remainder []byte

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			writeLine(mu, runLog, prefix, fmt.Sprintf("ERROR: recv: %v", err))
			return 1
		}
		if resp.Done {
			// Flush any remaining buffered bytes (no trailing newline).
			if len(remainder) > 0 {
				writeLine(mu, runLog, prefix, string(remainder))
				nodeLog.WriteString(string(remainder) + "\n") //nolint:errcheck
			}
			return int(resp.ExitCode)
		}

		// Split chunk into complete lines; carry any partial line forward.
		data := append(remainder, resp.Output...)
		scanner := bufio.NewScanner(bytes.NewReader(data))
		remainder = nil
		for scanner.Scan() {
			line := scanner.Text()
			writeLine(mu, runLog, prefix, line)
			fmt.Fprintln(nodeLog, line) //nolint:errcheck
		}
		// If data doesn't end with '\n', the last partial line is carried over.
		if len(data) > 0 && data[len(data)-1] != '\n' {
			// Re-extract the unterminated tail.
			lastNL := bytes.LastIndexByte(data, '\n')
			if lastNL >= 0 {
				remainder = data[lastNL+1:]
			} else {
				remainder = data
			}
		}
	}
	return 0
}

func writeLine(mu *sync.Mutex, w io.Writer, prefix, line string) {
	mu.Lock()
	fmt.Fprintf(w, "%s%s\n", prefix, line)
	mu.Unlock()
}
