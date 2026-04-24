// Package db manages job status updates in SkyPilot's SQLite jobs database.
// The schema and lock paths must stay in sync with sky/skylet/job_lib.py.
package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

// Open opens the SkyPilot jobs SQLite database.
func Open(dbPath string) (*sql.DB, error) {
	return sql.Open("sqlite", dbPath)
}

// SetRunning transitions job to RUNNING, setting start_at and clearing end_at.
func SetRunning(db *sql.DB, lockDir string, jobID int) error {
	return withLock(lockDir, jobID, func() error {
		now := float64(time.Now().UnixMilli()) / 1000.0
		_, err := db.Exec(
			"UPDATE jobs SET status='RUNNING', start_at=?, end_at=NULL WHERE job_id=?",
			now, jobID)
		return err
	})
}

// SetFinal transitions job to SUCCEEDED or FAILED, setting end_at.
// Matches job_lib.py: only updates if end_at IS NULL (idempotent).
func SetFinal(db *sql.DB, lockDir string, jobID int, succeeded bool) error {
	status := "SUCCEEDED"
	if !succeeded {
		status = "FAILED"
	}
	return withLock(lockDir, jobID, func() error {
		now := float64(time.Now().UnixMilli()) / 1000.0
		_, err := db.Exec(
			"UPDATE jobs SET status=?, end_at=? WHERE job_id=? AND end_at IS NULL",
			status, now, jobID)
		return err
	})
}

// SetExitCodes stores per-node exit codes for a job.
// Matches job_lib.set_exit_codes: stores as comma-separated string.
func SetExitCodes(db *sql.DB, lockDir string, jobID int, exitCodes []int) error {
	parts := make([]byte, 0, len(exitCodes)*4)
	for i, code := range exitCodes {
		if i > 0 {
			parts = append(parts, ',')
		}
		parts = fmt.Appendf(parts, "%d", code)
	}
	return withLock(lockDir, jobID, func() error {
		_, err := db.Exec(
			"UPDATE jobs SET exit_codes=? WHERE job_id=?",
			string(parts), jobID)
		return err
	})
}

// withLock acquires a per-job file lock (matching job_lib._JOB_STATUS_LOCK)
// before calling f, then releases it.
func withLock(lockDir string, jobID int, f func() error) error {
	if err := os.MkdirAll(lockDir, 0755); err != nil {
		return fmt.Errorf("mkdir lock dir: %w", err)
	}
	lockPath := filepath.Join(lockDir, fmt.Sprintf(".job_%d.lock", jobID))
	fh, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("open lock file: %w", err)
	}
	defer fh.Close()

	if err := syscall.Flock(int(fh.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("flock: %w", err)
	}
	defer syscall.Flock(int(fh.Fd()), syscall.LOCK_UN) //nolint:errcheck

	return f()
}
