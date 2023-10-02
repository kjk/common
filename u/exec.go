package u

import (
	"fmt"
	"os"
	"os/exec"
)

func RunLoggedInDir(dir string, exe string, args ...string) error {
	cmd := exec.Command(exe, args...)
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	return err
}

func RunLoggedInDirMust(dir string, exe string, args ...string) {
	Must(RunLoggedInDir(dir, exe, args...))
}

func RunMust(exe string, args ...string) string {
	cmd := exec.Command(exe, args...)
	d, err := cmd.CombinedOutput()
	out := string(d)
	PanicIf(err != nil, "'%s' failed with '%s', out:\n'%s'\n", cmd.String(), err, out)
	return out
}

func RunLoggedMust(exe string, args ...string) string {
	cmd := exec.Command(exe, args...)
	d, err := cmd.CombinedOutput()
	out := string(d)
	PanicIf(err != nil, "'%s' failed with '%s', out:\n'%s'\n", cmd.String(), err, out)
	fmt.Printf("%s:\n%s\n", cmd.String(), out)
	return out
}
