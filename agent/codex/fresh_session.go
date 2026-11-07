package codex

import (
	"fmt"
	"strings"
)

func (a *Agent) ValidateFreshSession() error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, arg := range a.cliExtraArgs {
		for _, flag := range []string{"resume", "--resume", "--session-id", "--thread-id"} {
			if arg == flag || strings.HasPrefix(arg, flag+"=") {
				return fmt.Errorf("fresh session conflicts with CLI argument %q", arg)
			}
		}
	}
	return nil
}
