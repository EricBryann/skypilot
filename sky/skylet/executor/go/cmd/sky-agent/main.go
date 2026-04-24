// sky-agent runs on every cluster node (head and workers) and accepts
// Execute RPCs from sky-exec on the head node. It runs the supplied bash
// script and streams stdout+stderr back as raw bytes.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"

	"google.golang.org/grpc"
	pb "skypilot.dev/executor/gen/agent"
	"skypilot.dev/executor/pkg/constants"
)

type agentServer struct {
	pb.UnimplementedAgentServiceServer
}

func (s *agentServer) Execute(req *pb.ExecuteRequest, stream pb.AgentService_ExecuteServer) error {
	// Write script to a temp file so bash can source it via stdin redirect.
	f, err := os.CreateTemp("", fmt.Sprintf("sky-job-%d-rank-%d-*.sh", req.JobId, req.NodeRank))
	if err != nil {
		return fmt.Errorf("create temp script: %w", err)
	}
	scriptPath := f.Name()
	defer os.Remove(scriptPath)

	if _, err := f.WriteString(req.Script); err != nil {
		f.Close()
		return fmt.Errorf("write script: %w", err)
	}
	f.Close()

	scriptFile, err := os.Open(scriptPath)
	if err != nil {
		return fmt.Errorf("open script: %w", err)
	}
	defer scriptFile.Close()

	// Run: bash -s < script  (non-interactive; conda env must already be set
	// in the script via explicit source commands, same as the Python path).
	cmd := exec.CommandContext(stream.Context(), "bash", "-s")
	cmd.Stdin = scriptFile

	// Pipe merged stdout+stderr through a goroutine that sends chunks to the
	// caller. We use a pipe so we can read incrementally.
	pr, pw := io.Pipe()
	cmd.Stdout = pw
	cmd.Stderr = pw

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start bash: %w", err)
	}

	// Send PID in the first response so sky-exec can include it in the prefix.
	if sendErr := stream.Send(&pb.ExecuteResponse{Pid: int32(cmd.Process.Pid)}); sendErr != nil {
		cmd.Process.Kill() //nolint:errcheck
		return sendErr
	}

	// Close the write end of the pipe when the process exits.
	waitDone := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		pw.Close()
		waitDone <- err
	}()

	// Stream output to caller in 32 KiB chunks.
	buf := make([]byte, 32*1024)
	for {
		n, readErr := pr.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&pb.ExecuteResponse{Output: buf[:n]}); sendErr != nil {
				cmd.Process.Kill() //nolint:errcheck
				<-waitDone
				return sendErr
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read pipe: %w", readErr)
		}
	}

	exitCode := 0
	if err := <-waitDone; err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			return fmt.Errorf("wait: %w", err)
		}
	}

	return stream.Send(&pb.ExecuteResponse{Done: true, ExitCode: int32(exitCode)})
}

func main() {
	port := flag.Int("port", constants.AgentPort, "Port to listen on")
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		log.Fatalf("sky-agent: listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterAgentServiceServer(s, &agentServer{})
	log.Printf("sky-agent: listening on :%d", *port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("sky-agent: serve: %v", err)
	}
}

// satisfy unused import for context (used via stream.Context())
var _ = context.Background
