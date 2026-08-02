package main

import (
	"context"
	"errors"
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
		"corex.participation.commands.v1.*.*",
		"rpc.corex.participation-replay.v1.*.*",
		"rpc.corex.participation-snapshot.v1.*.*",
		"$JS.ACK.INTERACTION_LOGS.fanout-projector.>",
		"$JS.ACK.PARTICIPATION_COMMANDS.relaypoint-participation.>",
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

func TestProjectorPostgresDSNUsesRequiredSecretFileArgument(t *testing.T) {
	path, err := postgresDSNPath([]string{"--postgres-dsn-file=/run/secrets/relaypoint-projector.dsn"})
	if err != nil || path != "/run/secrets/relaypoint-projector.dsn" {
		t.Fatalf("path=%q err=%v", path, err)
	}
	for _, arguments := range [][]string{nil, {"--postgres-dsn-file="}, {"--postgres-dsn=secret"}, {"--postgres-dsn-file=a", "extra"}} {
		if _, err := postgresDSNPath(arguments); err == nil {
			t.Fatalf("arguments accepted: %v", arguments)
		}
	}
}

func TestRunProjectorsFailsClosed(t *testing.T) {
	want := errors.New("consumer failed")
	blocked := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	if err := runProjectors(context.Background(), blocked, func(context.Context) error { return want }); !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if err := runProjectors(context.Background(), func(context.Context) error { return nil }, blocked); err == nil {
		t.Fatal("unexpected clean runtime exit accepted")
	}
	if err := runProjectors(context.Background()); err == nil {
		t.Fatal("empty runtime set accepted")
	}
	if err := runProjectors(context.Background(), nil); err == nil {
		t.Fatal("nil runtime accepted")
	}
}

func TestRunProjectorsStopsCleanlyWithParent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	run := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	if err := runProjectors(ctx, run, run); err != nil {
		t.Fatalf("error = %v", err)
	}
}
