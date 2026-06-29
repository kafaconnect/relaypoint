package authcallout

import (
	"testing"

	"github.com/nats-io/nats.go"
)

// @spec:obs.authcallout-extracts-trace-context
func TestTraceparentOf(t *testing.T) {
	tp := "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"

	withHeader := nats.NewMsg("$SYS.REQ.USER.AUTH")
	withHeader.Header.Set("traceparent", tp)

	cases := []struct {
		name string
		msg  *nats.Msg
		want string
	}{
		{"nil message", nil, ""},
		{"no header", &nats.Msg{Subject: "x"}, ""},
		{"header present", withHeader, tp},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := traceparentOf(c.msg); got != c.want {
				t.Fatalf("traceparentOf = %q, want %q", got, c.want)
			}
		})
	}
}
