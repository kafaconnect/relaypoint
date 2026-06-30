package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// @spec:deploy.nats.projector-user
func TestProjectorNATSUserDefaultIsProjector(t *testing.T) {
	if defaultNATSUser != "projector" {
		t.Errorf("default NATS_USER = %q, want projector (router was the diverged wrong default)", defaultNATSUser)
	}
	if defaultNATSPassword == "router-dev" {
		t.Errorf("default NATS_PASSWORD still the router credential %q", defaultNATSPassword)
	}
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
