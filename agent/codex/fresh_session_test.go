package codex

import "testing"

func TestFreshSessionRejectsResumeOverride(t *testing.T) {
	for _, arg := range []string{"--resume=old", "--session-id=old"} {
		a := &Agent{cliExtraArgs: []string{arg}}
		if a.ValidateFreshSession() == nil {
			t.Fatal("accepted", arg)
		}
	}
	if err := (&Agent{}).ValidateFreshSession(); err != nil {
		t.Fatal(err)
	}
}
