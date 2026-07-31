package authcallout

import (
	"os"
	"strings"
	"testing"
)

// @spec:substrate.no-domain-branch-static-review
func TestSubscriberAuthorizationHasNoRoleAgentBranch(t *testing.T) {
	files := []string{"grants.go", "visitor_token.go", "../signaling/router.go"}
	forbidden := []string{
		"case signaling.RoleAgent",
		"RoleOf(id) == RoleAgent",
		"RoleOf(id) == signaling.RoleAgent",
		"id.Role == RoleAgent",
		"id.Role == signaling.RoleAgent",
	}
	for _, file := range files {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, pattern := range forbidden {
			if strings.Contains(string(body), pattern) {
				t.Fatalf("%s contains forbidden authorization branch %q", file, pattern)
			}
		}
	}
}
