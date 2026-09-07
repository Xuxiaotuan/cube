package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cube-js/cube-operator/internal/agent"
)

func TestAuthorityStartupHelper(t *testing.T) {
	if os.Getenv("CUBE_AUTHORITY_STARTUP_HELPER") != "1" {
		t.Skip("subprocess only")
	}
	flag.CommandLine = flag.NewFlagSet("lease-agent", flag.ExitOnError)
	os.Args = []string{"lease-agent", "--backend=kubernetes", "--path=" + os.Getenv("AUTHORITY_TEST_LEADERSHIP"), "--promotion-path=" + os.Getenv("AUTHORITY_TEST_PROMOTION")}
	main()
	t.Fatal("invalid strict startup unexpectedly returned")
}

func TestAuthorityInvalidStartupFencesPriorFiles(t *testing.T) {
	for _, strict := range []string{"true", "invalid"} {
		t.Run(strict, func(t *testing.T) {
			dir := t.TempDir()
			leadershipPath, promotionPath := filepath.Join(dir, "leadership.json"), filepath.Join(dir, "promotion.json")
			for _, path := range []string{leadershipPath, promotionPath} {
				if err := os.WriteFile(path, []byte("previous-active"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAuthorityStartupHelper$", "-test.count=1")
			for _, entry := range os.Environ() {
				if !strings.HasPrefix(entry, "CUBESTORE_AUTHORITY_") && !strings.HasPrefix(entry, "CUBE_AUTHORITY_STARTUP_HELPER=") {
					cmd.Env = append(cmd.Env, entry)
				}
			}
			cmd.Env = append(cmd.Env, "CUBE_AUTHORITY_STARTUP_HELPER=1", "CUBESTORE_AUTHORITY_STRICT="+strict, "AUTHORITY_TEST_LEADERSHIP="+leadershipPath, "AUTHORITY_TEST_PROMOTION="+promotionPath)
			if err := cmd.Run(); err == nil {
				t.Fatal("invalid configuration did not stop startup")
			}
			if ctx.Err() != nil {
				t.Fatal("startup hung instead of rejecting configuration")
			}
			raw, err := os.ReadFile(leadershipPath)
			if err != nil {
				t.Fatal(err)
			}
			var file agent.LeadershipFile
			if err := json.Unmarshal(raw, &file); err != nil {
				t.Fatal(err)
			}
			if file.HolderID != "" || file.ExpiresAt.After(time.Now()) {
				t.Fatal("invalid strict configuration left active leadership")
			}
			raw, err = os.ReadFile(promotionPath)
			if err != nil {
				t.Fatal(err)
			}
			var marker map[string]any
			if err := json.Unmarshal(raw, &marker); err != nil {
				t.Fatal(err)
			}
			if marker["activeLeader"] != "" || marker["leaseToken"] != "" {
				t.Fatal("invalid strict configuration left promotion enabled")
			}
		})
	}
}
