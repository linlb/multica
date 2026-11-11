package agent

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Only fixture-owned files are written; no installed Codex or account is used.
func startupRecoveryFixture(t *testing.T, tail string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture is POSIX-only")
	}
	return writeFakeCodexAppServer(t, `
DIR="$(dirname "$0")"
ATTEMPT=$(cat "$DIR/attempts" 2>/dev/null || echo 0)
echo "$((ATTEMPT+1))" > "$DIR/attempts"
read line
echo '{"id":1,"result":{}}'
read line
read line
echo '{"id":2,"result":{"thread":{"id":"thr-recovery"}}}'
read line
echo '{"id":3,"result":{}}'
echo '{"method":"turn/started","params":{"threadId":"thr-recovery","turn":{"id":"turn-recovery"}}}'
echo 'ERROR codex_models_manager::manager: failed to refresh available models: request timed out' >&2
touch "$DIR/ready"
`+tail)
}

func waitCodexFixtureFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("fixture did not reach %s", filepath.Base(path))
}

func TestCodexCatalogRecoveryDoesNotReplayCleanupActivity(t *testing.T) {
	for _, tc := range []struct{ name, event string }{
		{"tool", `echo '{"method":"item/completed","params":{"threadId":"thr-recovery","turnId":"turn-recovery","item":{"type":"commandExecution","id":"write-1","aggregatedOutput":"written"}}}'`},
		{"approval", `echo '{"id":99,"method":"item/commandExecution/requestApproval","params":{"threadId":"thr-recovery","turnId":"turn-recovery"}}'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// EOF exposes activity after the watchdog stopped consuming its channel.
			fake := startupRecoveryFixture(t, `
while read line; do :; done
echo write >> "$DIR/business-writes"
`+tc.event+"\n")
			result := executeFakeCodex(t, fake, ExecOptions{
				Timeout: 4 * time.Second, FirstTurnNoProgressTimeout: 50 * time.Millisecond,
				SemanticInactivityTimeout: time.Second,
			})
			if result.Status != "timeout" {
				t.Fatalf("result = %+v", result)
			}
			assertCodexAttemptCount(t, fake, "1")
			writes, err := os.ReadFile(filepath.Join(filepath.Dir(fake), "business-writes"))
			if err != nil || string(writes) != "write\n" {
				t.Fatalf("business writes = %q, err = %v; want exactly one", writes, err)
			}
		})
	}
}

func TestCodexCatalogRecoveryDoesNotDiscardSupplement(t *testing.T) {
	for _, tc := range []struct {
		name, response string
		wantError      bool
	}{
		{"accepted", `echo '{"id":4,"result":{"turnId":"turn-recovery"}}'`, false},
		{"rejected", `echo '{"id":4,"error":{"code":-32000,"message":"ambiguous failure"}}'`, true},
		{"response during cleanup", `while read line; do :; done
echo '{"id":4,"result":{"turnId":"turn-recovery"}}'`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := startupRecoveryFixture(t, `
read line
echo "$line" > "$DIR/supplement"
`+tc.response+`
while read line; do :; done
`)
			backend, err := New("codex", Config{ExecutablePath: fake, Logger: slog.Default()})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			session, err := backend.Execute(ctx, "original prompt", ExecOptions{
				FirstTurnNoProgressTimeout: 300 * time.Millisecond, SemanticInactivityTimeout: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			go func() {
				for range session.Messages {
				}
			}()
			waitCodexFixtureFile(t, filepath.Join(filepath.Dir(fake), "ready"))
			deadline := time.Now().Add(time.Second)
			for !session.SupplementReady() && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if !session.SupplementReady() {
				t.Fatal("supplement did not become ready")
			}
			err = session.Supplement(ctx, "new constraint")
			if (err != nil) != tc.wantError {
				t.Fatalf("supplement error = %v", err)
			}
			select {
			case result := <-session.Result:
				if result.Status != "timeout" {
					t.Fatalf("result = %+v", result)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("supplement/cleanup did not exit within budget")
			}
			assertCodexAttemptCount(t, fake, "1")
			instruction, err := os.ReadFile(filepath.Join(filepath.Dir(fake), "supplement"))
			if err != nil || !strings.Contains(string(instruction), "new constraint") || !strings.Contains(string(instruction), "turn/steer") {
				t.Fatalf("supplement request = %s, error = %v", instruction, err)
			}
			if session.SupplementReady() {
				t.Fatal("retired attempt still accepts supplements")
			}
			if err := session.Supplement(ctx, "too late"); err == nil {
				t.Fatal("retired attempt accepted supplement")
			}
		})
	}
}

func TestCodexCatalogRecoveryCancellationDuringCleanup(t *testing.T) {
	fake := startupRecoveryFixture(t, `
while read line; do :; done
touch "$DIR/cleanup"
while [ ! -f "$DIR/release" ]; do sleep 0.01; done
`)
	backend, err := New("codex", Config{ExecutablePath: fake, Logger: slog.Default()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session, err := backend.Execute(ctx, "prompt", ExecOptions{
		Timeout: 4 * time.Second, FirstTurnNoProgressTimeout: 50 * time.Millisecond,
		SemanticInactivityTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for range session.Messages {
		}
	}()
	dir := filepath.Dir(fake)
	waitCodexFixtureFile(t, filepath.Join(dir, "cleanup"))
	cancel()
	if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-session.Result:
		if result.Status != "cancelled" {
			t.Fatalf("result = %+v; want cancellation to prevent recovery", result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled recovery did not exit within its cleanup budget")
	}
	assertCodexAttemptCount(t, fake, "1")
}

type codexRecoveryLogWriter func([]byte) (int, error)

func (w codexRecoveryLogWriter) Write(p []byte) (int, error) { return w(p) }

func TestCodexCatalogRecoveryCancellationAtBackoff(t *testing.T) {
	fake := startupRecoveryFixture(t, "while read line; do :; done\n")
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(codexRecoveryLogWriter(func(p []byte) (int, error) {
		if strings.Contains(string(p), "codex retry scheduled") {
			cancel()
		}
		return len(p), nil
	}), nil))
	backend, err := New("codex", Config{ExecutablePath: fake, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	session, err := backend.Execute(ctx, "prompt", ExecOptions{
		FirstTurnNoProgressTimeout: 50 * time.Millisecond, SemanticInactivityTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for range session.Messages {
		}
	}()
	select {
	case result := <-session.Result:
		if result.Status != "cancelled" {
			t.Fatalf("result = %+v; want cancellation at retry backoff", result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled backoff did not return")
	}
	assertCodexAttemptCount(t, fake, "1")
}

func TestCodexTimeoutReportsObservedPhase(t *testing.T) {
	for _, tc := range []struct {
		name, events, phase, tools, attempts string
	}{
		{
			name: "heartbeat and stderr are not progress", phase: "waiting_first_event", tools: "0", attempts: "2",
			events: `echo '{"method":"heartbeat","params":{}}'
echo 'stderr noise' >&2`,
		},
		{
			name: "reasoning then silence", phase: "waiting_after_progress", tools: "0", attempts: "1",
			events: `echo '{"method":"item/started","params":{"threadId":"thr-recovery","turnId":"turn-recovery","item":{"type":"reasoning","id":"reason-1"}}}'`,
		},
		{
			name: "quiet tool still running", phase: "tool_execution", tools: "1", attempts: "1",
			events: `echo '{"method":"item/started","params":{"threadId":"thr-recovery","turnId":"turn-recovery","item":{"type":"commandExecution","id":"cmd-1","command":"fixture-only"}}}'`,
		},
		{
			name: "tool completed then silence", phase: "waiting_after_progress", tools: "0", attempts: "1",
			events: `echo '{"method":"item/started","params":{"threadId":"thr-recovery","turnId":"turn-recovery","item":{"type":"commandExecution","id":"cmd-1","command":"fixture-only"}}}'
echo '{"method":"item/completed","params":{"threadId":"thr-recovery","turnId":"turn-recovery","item":{"type":"commandExecution","id":"cmd-1","aggregatedOutput":"done"}}}'`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := startupRecoveryFixture(t, tc.events+"\nwhile read line; do :; done\n")
			result := executeFakeCodex(t, fake, ExecOptions{
				Timeout: 5 * time.Second, FirstTurnNoProgressTimeout: 100 * time.Millisecond,
				SemanticInactivityTimeout: 300 * time.Millisecond,
			})
			if result.Status != "timeout" {
				t.Fatalf("result = %+v", result)
			}
			for _, field := range []string{`phase="` + tc.phase + `"`, "in_flight_tools=" + tc.tools, "waited="} {
				if !strings.Contains(result.Error, field) {
					t.Fatalf("diagnostic missing %q: %s", field, result.Error)
				}
			}
			if strings.Contains(result.Error, "waited=0s") {
				t.Fatalf("diagnostic must report observed wait: %s", result.Error)
			}
			assertCodexAttemptCount(t, fake, tc.attempts)
		})
	}
}

func TestCodexSlowFirstEventDisarmsStartupDeadline(t *testing.T) {
	fake := startupRecoveryFixture(t, `
sleep 0.1
echo '{"method":"item/started","params":{"threadId":"thr-recovery","turnId":"turn-recovery","item":{"type":"reasoning","id":"reason-1"}}}'
sleep 0.3
echo '{"method":"turn/completed","params":{"threadId":"thr-recovery","turn":{"id":"turn-recovery","status":"completed"}}}'
`)
	result := executeFakeCodex(t, fake, ExecOptions{
		Timeout: 4 * time.Second, FirstTurnNoProgressTimeout: 250 * time.Millisecond,
		SemanticInactivityTimeout: time.Second,
	})
	if result.Status != "completed" {
		t.Fatalf("slow but active turn failed: %+v", result)
	}
	assertCodexAttemptCount(t, fake, "1")
}

func TestCodexCatalogRecoveryRetiresSessionBeforeBackoff(t *testing.T) {
	fake := startupRecoveryFixture(t, `
if [ "$ATTEMPT" = 1 ]; then
  echo '{"method":"turn/completed","params":{"threadId":"thr-recovery","turn":{"id":"turn-recovery","status":"completed"}}}'
fi
while read line; do :; done
`)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	var session *Session
	published := make(chan struct{})
	checked := make(chan struct{}, 1)
	logger := slog.New(slog.NewTextHandler(codexRecoveryLogWriter(func(p []byte) (int, error) {
		if strings.Contains(string(p), "codex retry scheduled") {
			<-published
			if session.SupplementReady() {
				t.Error("failed attempt still ready during backoff")
			}
			if err := session.Supplement(ctx, "late instruction"); err == nil {
				t.Error("failed attempt accepted instruction during backoff")
			}
			checked <- struct{}{}
		}
		return len(p), nil
	}), nil))
	backend, err := New("codex", Config{ExecutablePath: fake, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	session, err = backend.Execute(ctx, "prompt", ExecOptions{
		FirstTurnNoProgressTimeout: 50 * time.Millisecond, SemanticInactivityTimeout: time.Second,
	})
	close(published)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for range session.Messages {
		}
	}()
	select {
	case result := <-session.Result:
		if result.Status != "completed" {
			t.Fatalf("result = %+v", result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("recovery did not finish")
	}
	select {
	case <-checked:
	default:
		t.Fatal("backoff was not observed")
	}
	assertCodexAttemptCount(t, fake, "2")
}
