package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSessionNewParsingDefaultsOverridesAndValidation(t *testing.T) {
	t.Setenv("CC_PROJECT", "project")
	t.Setenv("CC_SESSION_KEY", "key")
	t.Setenv("CC_SESSION_ID", "s7")
	got, err := parseSessionNew([]string{"--prompt", " exact text ", "--json"})
	if err != nil || got.Request.ExpectedSessionID != "s7" || got.Request.Handoff != " exact text " || !got.JSON {
		t.Fatal(got, err)
	}
	t.Setenv("CC_DATA_DIR", "/source")
	override, err := parseSessionNew([]string{"--prompt", "text", "--data-dir", "/isolated"})
	if err != nil || override.Request.ExpectedSessionID != "" {
		t.Fatal("cross-instance source ID reused", override, err)
	}
	got, err = parseSessionNew([]string{"--prompt", "text", "-s", "other", "-p", "other-project", "--data-dir", "/isolated"})
	if err != nil || got.Request.ExpectedSessionID != "" || got.DataDir != "/isolated" {
		t.Fatal(got, err)
	}
	file := filepath.Join(t.TempDir(), "handoff.md")
	if err = os.WriteFile(file, []byte("from file\n"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err = parseSessionNew([]string{"--handoff", file, "--name", "next"})
	if err != nil || got.Request.Handoff != "from file\n" || got.Request.Name != "next" {
		t.Fatal(got, err)
	}
	for _, args := range [][]string{{}, {"--prompt", ""}, {"--prompt", "a", "--handoff", file}, {"--prompt", strings.Repeat("x", 65537)}, {"--unknown"}, {"--handoff", file, "extra"}} {
		if _, err = parseSessionNew(args); err == nil {
			t.Fatal("accepted invalid args", args)
		}
	}
}
