//go:build unix

package leadership

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"testing"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

// This helper is a local test subprocess, NOT an Operator Manager or a Pod.
// SetupSignalHandler is the exact controller-runtime signal entrypoint used by
// main. Kubernetes Manager election/loss is deliberately not claimed here.
func TestLeaseLifecycleChild(t *testing.T) {
	if os.Getenv("CUBE_HA_LIFECYCLE_HELPER") != "1" {
		t.Skip("subprocess only")
	}
	ctx := ctrl.SetupSignalHandler()
	command := make(chan string, 1)
	go func() { line, _ := bufio.NewReader(os.Stdin).ReadString('\n'); command <- line }()
	fmt.Println("READY")
	select {
	case <-ctx.Done():
		fmt.Println("CANCELLED")
	case line := <-command:
		if line != "replay\n" {
			t.Fatal("unexpected helper command")
		}
		fmt.Println("REPLAY_OLD_IDENTITY")
	}
}

type lifecycleChild struct {
	cmd    *exec.Cmd
	input  io.WriteCloser
	lines  <-chan string
	ctx    context.Context
	waited bool
}

func startLifecycleChild(t *testing.T) *lifecycleChild {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLeaseLifecycleChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), "CUBE_HA_LIFECYCLE_HELPER=1")
	input, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	lines := make(chan string, 8)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	child := &lifecycleChild{cmd: cmd, input: input, lines: lines, ctx: ctx}
	t.Cleanup(func() {
		if !child.waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
		_ = input.Close()
		cancel()
	})
	child.await(t, "READY")
	return child
}

func (c *lifecycleChild) await(t *testing.T, expected string) {
	t.Helper()
	for {
		select {
		case line, ok := <-c.lines:
			if !ok {
				t.Fatalf("child exited before %s", expected)
			}
			if line == expected {
				return
			}
		case <-c.ctx.Done():
			t.Fatalf("child timed out before %s", expected)
		}
	}
}

func TestLeaseLifecycleSignals(t *testing.T) {
	for _, signal := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL} {
		t.Run(signal.String(), func(t *testing.T) {
			child := startLifecycleChild(t)
			if err := child.cmd.Process.Signal(signal); err != nil {
				t.Fatal(err)
			}
			if signal != syscall.SIGKILL {
				child.await(t, "CANCELLED")
			}
			err := child.cmd.Wait()
			child.waited = true
			if signal == syscall.SIGKILL {
				var exit *exec.ExitError
				if !errors.As(err, &exit) {
					t.Fatalf("SIGKILL did not terminate child: %v", err)
				}
				status, ok := exit.Sys().(syscall.WaitStatus)
				if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
					t.Fatal("child did not exit from SIGKILL")
				}
			} else if err != nil {
				t.Fatalf("signal cancellation failed: %v", err)
			}
			t.Logf("AUDIT localChildSignal=%s gracefulCancellation=%t managerElectionTested=false", signal, signal != syscall.SIGKILL)
		})
	}
}

func TestLeaseLifecyclePausedWriterModel(t *testing.T) {
	store, writer, authority, old := authorityFixture(t)
	child := startLifecycleChild(t)
	if err := child.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	var status syscall.WaitStatus
	var waitedPID int
	var waitErr error
	for {
		waitedPID, waitErr = syscall.Wait4(child.cmd.Process.Pid, &status, syscall.WUNTRACED, nil)
		if !errors.Is(waitErr, syscall.EINTR) {
			break
		}
	}
	stopped := status.Stopped() && status.StopSignal() == syscall.SIGSTOP
	if runtime.GOOS == "darwin" {
		// Darwin encodes SIGSTOP as 0x117f. Go's BSD Stopped method
		// excludes SIGSTOP, whereas Darwin's continued marker is SIGCONT.
		// Check the exact requested stop, not any non-exited status.
		stopped = uint32(status)&0xff == 0x7f && syscall.Signal((uint32(status)>>8)&0xff) == syscall.SIGSTOP
	}
	t.Logf("AUDIT pause expectedPID=%d returnedPID=%d rawStatus=0x%x platform=%s stoppedBySIGSTOP=%t waitErr=%v", child.cmd.Process.Pid, waitedPID, uint32(status), runtime.GOOS, stopped, waitErr)
	if waitErr != nil || waitedPID != child.cmd.Process.Pid || !stopped {
		t.Fatalf("child was not confirmed stopped: expectedPID=%d returnedPID=%d rawStatus=0x%x err=%v", child.cmd.Process.Pid, waitedPID, uint32(status), waitErr)
	}
	// The process pause is real. Authority and the resumed identity submission
	// are a fake-client model, not proof of a paused real Manager's isolation.
	advanceAuthority(t, authority)
	if _, err := io.WriteString(child.input, "replay\n"); err != nil {
		t.Fatal(err)
	}
	if err := child.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	child.await(t, "REPLAY_OLD_IDENTITY")
	if err := child.cmd.Wait(); err != nil {
		child.waited = true
		t.Fatal(err)
	}
	child.waited = true
	if _, ok, err := store.Renew(context.Background(), old, time.Minute); err != nil || ok {
		t.Fatal("resumed old identity renewed")
	}
	if err := store.Release(context.Background(), old); !errors.Is(err, ErrStaleLease) {
		t.Fatal("resumed old identity released new lease")
	}
	if writer.updates != 0 {
		t.Fatal("stale identity reached writer")
	}
	t.Log("AUDIT localChildPause=confirmed resumedOldIdentity=rejected authority=fake managerElectionTested=false")
}
