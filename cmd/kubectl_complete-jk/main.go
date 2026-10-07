// Command kubectl_complete-jk makes `kubectl jk <TAB>` complete. kubectl runs an executable with this name when it
// completes a plugin's arguments, and expects the output of kubectl-jk's hidden `__complete` command.
package main

import (
	"os"
	"os/exec"
)

func main() {
	cmd := exec.Command("kubectl-jk", append([]string{"__complete"}, os.Args[1:]...)...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			os.Exit(ee.ExitCode())
		}
		os.Exit(1)
	}
}
