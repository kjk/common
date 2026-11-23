package u

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/kjk/common/assert"
)

func TestParseGitStatusZ(t *testing.T) {
	out := " M mod.txt\x00MM both.txt\x00A  added.txt\x00AM added-mod.txt\x00 D del.txt\x00" +
		"R  new name.txt\x00old name.txt\x00?? untracked.txt\x00UU conflict.txt\x00 T type.txt\x00"
	changes, err := parseGitStatusZ([]byte(out), false)
	assert.NoError(t, err)
	assert.Equal(t, 8, len(changes))

	byPath := map[string]*gitChange{}
	for _, c := range changes {
		byPath[c.Path] = c
	}
	assert.True(t, byPath["mod.txt"].InHead)
	assert.True(t, byPath["both.txt"].InHead)
	assert.False(t, byPath["added.txt"].InHead)
	assert.False(t, byPath["added-mod.txt"].InHead)
	assert.True(t, byPath["del.txt"].InHead)
	assert.True(t, byPath["conflict.txt"].InHead)
	assert.True(t, byPath["type.txt"].InHead)
	ren := byPath["new name.txt"]
	assert.Equal(t, "old name.txt", ren.OrigPath)
	assert.Equal(t, "old name.txt", ren.headPath())
	assert.Nil(t, byPath["untracked.txt"])

	changes, err = parseGitStatusZ([]byte(out), true)
	assert.NoError(t, err)
	assert.Equal(t, 9, len(changes))

	_, err = parseGitStatusZ([]byte("bad\x00"), false)
	assert.Error(t, err)
}

func gitMust(t *testing.T, dir string, args ...string) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, out)
	}
}

func TestCopyChanges(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not found")
	}
	repo := t.TempDir()
	write := func(name, s string) {
		writeFileMkdirMust(filepath.Join(repo, filepath.FromSlash(name)), []byte(s))
	}
	gitMust(t, repo, "init", "-q")
	gitMust(t, repo, "config", "user.email", "test@example.com")
	gitMust(t, repo, "config", "user.name", "test")
	gitMust(t, repo, "config", "core.autocrlf", "false")
	write("mod.txt", "mod v1")
	write("del.txt", "del v1")
	write("old.txt", "renamed content")
	write("a/same.txt", "a v1")
	write("b/same.txt", "b v1")
	gitMust(t, repo, "add", "-A")
	gitMust(t, repo, "commit", "-q", "-m", "init")

	write("mod.txt", "mod v2")
	os.Remove(filepath.Join(repo, "del.txt"))
	gitMust(t, repo, "mv", "old.txt", "new.txt")
	write("added.txt", "added")
	gitMust(t, repo, "add", "added.txt")
	write("added.txt", "added modified") // "AM"
	write("a/same.txt", "a v2")
	write("b/same.txt", "b v2")
	write("untracked.txt", "untracked")

	gitRoot := gitRootMustInDir(t, gitPath, repo)
	out, err := runCmdInDir(gitRoot, gitPath, "status", "--porcelain", "-z")
	assert.NoError(t, err)
	changes, err := parseGitStatusZ(out, false)
	assert.NoError(t, err)

	dir := t.TempDir()
	for _, c := range changes {
		copyChangeMust(gitPath, gitRoot, dir, c)
	}
	dirBefore, dirAfter := getBeforeAfterDirs(dir)
	read := func(dir, name string) string {
		d, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(name)))
		assert.NoError(t, err, name)
		return string(d)
	}
	assert.Equal(t, "mod v1", read(dirBefore, "mod.txt"))
	assert.Equal(t, "mod v2", read(dirAfter, "mod.txt"))
	assert.Equal(t, "del v1", read(dirBefore, "del.txt"))
	assert.Equal(t, "", read(dirAfter, "del.txt"))
	assert.Equal(t, "renamed content", read(dirBefore, "new.txt"))
	assert.Equal(t, "renamed content", read(dirAfter, "new.txt"))
	assert.Equal(t, "", read(dirBefore, "added.txt"))
	assert.Equal(t, "added modified", read(dirAfter, "added.txt"))
	// same file name in different dirs doesn't collide
	assert.Equal(t, "a v1", read(dirBefore, "a/same.txt"))
	assert.Equal(t, "b v2", read(dirAfter, "b/same.txt"))
	assert.False(t, FileExists(filepath.Join(dirAfter, "untracked.txt")))
}

func gitRootMustInDir(t *testing.T, gitPath, dir string) string {
	out, err := runCmdInDir(dir, gitPath, "rev-parse", "--show-toplevel")
	assert.NoError(t, err)
	return filepath.FromSlash(string(trimSpaceBytes(out)))
}

func trimSpaceBytes(d []byte) []byte {
	for len(d) > 0 && (d[len(d)-1] == '\n' || d[len(d)-1] == '\r' || d[len(d)-1] == ' ') {
		d = d[:len(d)-1]
	}
	return d
}
