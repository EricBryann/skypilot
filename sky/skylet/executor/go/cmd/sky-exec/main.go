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
	"sort"
	"strings"
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
	JobID          int      `json:"job_id"`
	AllNodeIPs     []string `json:"all_node_ips"`      // all cluster IPs; sky-exec selects NumNodes
	NumNodes       int      `json:"num_nodes"`         // how many nodes the task needs
	NumGPUsPerNode int      `json:"num_gpus_per_node"` // 0 for CPU-only tasks
	Script         string   `json:"script"`            // shared script; sky-exec injects per-node env vars
	LogDir         string   `json:"log_dir"`
	AgentPort      int      `json:"agent_port"`  // default 50052
	SkyletPort     int      `json:"skylet_port"` // default 46590
}

// nodeLogPath returns the log file path for a given rank, matching the
// convention in cloud_vm_ray_backend._build_go_config.
func nodeLogPath(logDir string, rank, numNodes int) string {
	if numNodes == 1 {
		return filepath.Join(logDir, "run.log")
	}
	nodeName := fmt.Sprintf("worker%d", rank)
	if rank == 0 {
		nodeName = "head"
	}
	return filepath.Join(logDir, fmt.Sprintf("%d-%s.log", rank, nodeName))
}

// shellQuote wraps s in single quotes, escaping any embedded single quotes.
// Matches Python's shlex.quote used by log_lib.make_task_bash_script.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

// queryFreeGPUs calls GetResources on one agent node and returns its free GPU count.
// Returns 0 on any error so the node is treated as having no available resources.
func queryFreeGPUs(ctx context.Context, ip string, agentPort int) int32 {
	addr := fmt.Sprintf("%s:%d", ip, agentPort)
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		log.Printf("sky-exec: resource query dial %s: %v", addr, err)
		return 0
	}
	defer conn.Close()
	resp, err := pb.NewAgentServiceClient(conn).GetResources(ctx, &pb.GetResourcesRequest{})
	if err != nil {
		log.Printf("sky-exec: GetResources %s: %v", addr, err)
		return 0
	}
	return resp.FreeGpus
}

// selectNodes queries all cluster nodes in parallel and returns the best
// NumNodes IPs whose free GPU count satisfies NumGPUsPerNode.
// Falls back to the first NumNodes IPs if not enough nodes qualify.
func selectNodes(ctx context.Context, cfg Config) []string {
	type result struct {
		ip       string
		freeGPUs int32
	}
	results := make([]result, len(cfg.AllNodeIPs))
	var wg sync.WaitGroup
	for i, ip := range cfg.AllNodeIPs {
		wg.Add(1)
		go func(i int, ip string) {
			defer wg.Done()
			results[i] = result{ip, queryFreeGPUs(ctx, ip, cfg.AgentPort)}
		}(i, ip)
	}
	wg.Wait()

	// Collect nodes that meet the GPU requirement.
	var candidates []result
	for _, r := range results {
		if int(r.freeGPUs) >= cfg.NumGPUsPerNode {
			candidates = append(candidates, r)
		}
	}

	// Sort descending by free GPUs — prefer nodes with the most headroom.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].freeGPUs > candidates[j].freeGPUs
	})

	selected := make([]string, 0, cfg.NumNodes)
	for _, c := range candidates {
		if len(selected) == cfg.NumNodes {
			break
		}
		selected = append(selected, c.ip)
	}

	return selected
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
func scheduleStep(skyletPort int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, fmt.Sprintf("localhost:%d", skyletPort),
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
	if cfg.SkyletPort == 0 {
		cfg.SkyletPort = 46590
	}
	// Expand ~ in paths — Go does not expand tilde automatically.
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("sky-exec: get home dir: %v", err)
	}
	if strings.HasPrefix(cfg.LogDir, "~/") {
		cfg.LogDir = filepath.Join(home, cfg.LogDir[2:])
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

	nodeIPs := selectNodes(ctx, cfg)
	nodeIPsEnv := strings.Join(nodeIPs, "\n")

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
	n := len(nodeIPs)
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

	exitCodes := make([]int, n)
	var wg sync.WaitGroup

	for i, ip := range nodeIPs {
		wg.Add(1)
		go func(rank int, ip string) {
			defer wg.Done()
			logPath := nodeLogPath(cfg.LogDir, rank, n)
			exitCodes[rank] = runOnNode(ctx, cfg, ip, rank, nodeIPsEnv, n, logPath, runLog, &mu)
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

	if err := db.SetFinal(database, lockDir, cfg.JobID, succeeded); err != nil {
		log.Printf("sky-exec: set final status: %v", err)
	}

	if err := db.SetExitCodes(database, lockDir, cfg.JobID, exitCodes); err != nil {
		log.Printf("sky-exec: set exit codes: %v", err)
	}

	go scheduleStep(cfg.SkyletPort)

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
func runOnNode(ctx context.Context, cfg Config, ip string, rank int, nodeIPsEnv string, numNodes int, logPath string, runLog io.Writer, mu *sync.Mutex) int {
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
	if err := os.MkdirAll(filepath.Dir(logPath), 0755); err != nil {
		writeLine(mu, runLog, nodePrefix(rank, ip, 0), fmt.Sprintf("ERROR: mkdir node log dir: %v", err))
		return 1
	}
	nodeLog, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		writeLine(mu, runLog, nodePrefix(rank, ip, 0), fmt.Sprintf("ERROR: open node log: %v", err))
		return 1
	}
	defer nodeLog.Close()

	// Inject per-node cluster env vars. Names match constants in
	// sky/skylet/constants.py; values are shell-quoted like Python's shlex.quote.
	script := fmt.Sprintf(
		"export SKYPILOT_NODE_RANK=%d\nexport SKYPILOT_NODE_IPS=%s\nexport SKYPILOT_NUM_NODES=%d\n%s",
		rank, shellQuote(nodeIPsEnv), numNodes, cfg.Script)

	client := pb.NewAgentServiceClient(conn)
	stream, err := client.Execute(ctx, &pb.ExecuteRequest{
		Script:   script,
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
