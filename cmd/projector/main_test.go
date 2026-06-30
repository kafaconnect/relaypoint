package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// @spec:deploy.nats.projector-user
func TestProjectorNATSUserDefaultIsProjector(t *testing.T) {
	if defaultNATSUser != "projector" {
		t.Errorf("default NATS_USER = %q, want projector (router was the diverged wrong default)", defaultNATSUser)
	}
}

// @spec:router.config.fail-loud-password
func TestNATSPasswordFailLoud(t *testing.T) {
	if os.Getenv("RH11_FAILLOUD") == "1" {
		mustEnv("NATS_PASSWORD") // child: NATS_PASSWORD unset → must os.Exit(1)
		return
	}
	if got := runFailLoudChild(t); got == nil {
		t.Fatal("mustEnv(NATS_PASSWORD) with the var unset must exit non-zero, but the child exited 0")
	}
}

// runFailLoudChild re-execs this test binary running only the fail-loud test, with NATS_PASSWORD
// stripped and RH11_FAILLOUD=1 so the child takes the os.Exit branch; it returns the non-nil exit error.
func runFailLoudChild(t *testing.T) error {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestNATSPasswordFailLoud")
	env := make([]string, 0, len(os.Environ())+1)
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "NATS_PASSWORD=") {
			continue
		}
		env = append(env, e)
	}
	cmd.Env = append(env, "RH11_FAILLOUD=1")
	err := cmd.Run()
	if exitErr, ok := err.(*exec.ExitError); ok && !exitErr.Success() {
		return exitErr
	}
	return nil
}

// @spec:deploy.nats.projector-user
func TestNATSConfDefinesLeastPrivilegeProjectorUser(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "deploy", "nats", "nats-server.conf"))
	if err != nil {
		t.Fatalf("read nats-server.conf: %v", err)
	}
	conf := string(b)

	if !strings.Contains(conf, `user: "projector"`) {
		t.Fatal("nats-server.conf has no `projector` user")
	}
	for _, want := range []string{
		"tenant.*.agent.*.feed.*",
		"tenant.*.agent.dlq.feed",
		"tenant.*.interaction.*.log",
		"$KV.projector-lease.>",
		"$KV.projector-snapshot.>",
	} {
		if !strings.Contains(conf, want) {
			t.Errorf("projector perms missing %q", want)
		}
	}
	if !strings.Contains(conf, "auth_users: [ router, projector,") {
		t.Error("projector must be a callout-exempt auth_users member")
	}
	if !strings.Contains(strings.ToLower(conf), "anonymous") {
		t.Error("infra-NATS anonymous posture must be documented in the conf")
	}
}
