package u

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

var (
	must = Must
)

/*
A tool to preview changes before checkin.
Uses WinMerge to do the diffing (https://winmerge.org/)
Another option that wouldn't require winmerge is to make it a web server,
implement web-based ui and launch the browser.
*/

// gitChange represents a single changed file in `git status`
type gitChange struct {
	// path relative to the root of the repository, '/' separated
	Path string
	// for renames and copies, the original path (in HEAD)
	OrigPath string
	// true if the file is in HEAD (i.e. not added / untracked)
	InHead bool
	// true if file is untracked (not checked in)
	Untracked bool
}

// headPath returns path of the file in HEAD
func (c *gitChange) headPath() string {
	if c.OrigPath != "" {
		return c.OrigPath
	}
	return c.Path
}

// parseGitStatusZ parses output of `git status --porcelain -z`
// Format of each entry: "XY path\x00", for renames and copies: "XY path\x00origPath\x00"
// See https://git-scm.com/docs/git-status#_short_format
func parseGitStatusZ(out []byte, includeUntracked bool) ([]*gitChange, error) {
	var res []*gitChange
	entries := bytes.Split(out, []byte{0})
	for i := 0; i < len(entries); i++ {
		e := string(entries[i])
		if e == "" {
			continue
		}
		if len(e) < 4 || e[2] != ' ' {
			return nil, fmt.Errorf("invalid git status entry: '%s'", e)
		}
		x, y := e[0], e[1]
		c := &gitChange{
			Path: e[3:],
		}
		if x == 'R' || x == 'C' || y == 'R' || y == 'C' {
			// next entry is the original path
			i++
			if i >= len(entries) {
				return nil, fmt.Errorf("missing original path for rename entry: '%s'", e)
			}
			c.OrigPath = string(entries[i])
		}
		switch {
		case x == '!':
			// ignored file
			continue
		case x == '?':
			c.Untracked = true
			if !includeUntracked {
				continue
			}
		case x == 'A':
			// added to index (also "AA" i.e. unmerged, both added)
			c.InHead = false
		case x == 'C':
			// copy: the new file is not in HEAD
			c.InHead = false
			c.OrigPath = ""
		default:
			// modified, deleted, renamed, type changed, unmerged etc.
			c.InHead = true
		}
		res = append(res, c)
	}
	return res, nil
}

func detectExeMust(name string) string {
	path, err := exec.LookPath(name)
	if err == nil {
		fmt.Printf("'%s' is '%s'\n", name, path)
		return path
	}
	fmt.Printf("Couldn't find '%s'\n", name)
	must(err)
	return ""
}

func detectWinMergeMust() string {
	var candidates []string
	for _, env := range []string{"ProgramFiles", "ProgramFiles(x86)"} {
		if dir := os.Getenv(env); dir != "" {
			candidates = append(candidates, filepath.Join(dir, "WinMerge", "WinMergeU.exe"))
		}
	}
	if dir := os.Getenv("LOCALAPPDATA"); dir != "" {
		candidates = append(candidates, filepath.Join(dir, "Programs", "WinMerge", "WinMergeU.exe"))
	}
	for _, path := range candidates {
		if FileExists(path) {
			return path
		}
	}
	return detectExeMust("WinMergeU")
}

func runCmdInDir(dir string, exePath string, args ...string) ([]byte, error) {
	cmd := exec.Command(exePath, args...)
	cmd.Dir = dir
	fmt.Printf("running: %s %v\n", filepath.Base(exePath), args)
	return cmd.Output()
}

func gitRootMust(gitPath string) string {
	out, err := runCmdInDir("", gitPath, "rev-parse", "--show-toplevel")
	must(err)
	root := strings.TrimSpace(string(out))
	PanicIf(root == "", "git rev-parse --show-toplevel returned empty string")
	return filepath.FromSlash(root)
}

func getBeforeAfterDirs(dir string) (string, string) {
	dirBefore := filepath.Join(dir, "before")
	dirAfter := filepath.Join(dir, "after")
	return dirBefore, dirAfter
}

// https://manual.winmerge.org/Command_line.html
func runWinMergeMust(winMergePath string, dir string) {
	dirBefore, dirAfter := getBeforeAfterDirs(dir)
	/*
		/e : close with Esc
		/u : don't add paths to MRU
		/wl, wr : open left/right as read-only
		/r : recursive compare
	*/
	args := []string{"/e", "/u", "/wl", "/wr", "/r", dirBefore, dirAfter}
	cmd := exec.Command(winMergePath, args...)
	fmt.Printf("running: %s %v\n", filepath.Base(winMergePath), args)
	must(cmd.Start())
}

func writeFileMkdirMust(path string, d []byte) {
	must(os.MkdirAll(filepath.Dir(path), 0755))
	must(os.WriteFile(path, d, 0644))
}

// copyChangeMust writes HEAD version of the file to dirBefore and current
// version to dirAfter. If the file doesn't exist in HEAD (added) or on disk
// (deleted), an empty file is written instead.
// Files keep their relative path so that files with the same name in
// different directories don't collide.
func copyChangeMust(gitPath, gitRoot, dir string, change *gitChange) {
	dirBefore, dirAfter := getBeforeAfterDirs(dir)
	relPath := filepath.FromSlash(change.Path)

	var before []byte
	if change.InHead {
		var err error
		before, err = runCmdInDir(gitRoot, gitPath, "show", "HEAD:"+change.headPath())
		must(err)
	}
	writeFileMkdirMust(filepath.Join(dirBefore, relPath), before)

	var after []byte
	diskPath := filepath.Join(gitRoot, relPath)
	if FileExists(diskPath) {
		var err error
		after, err = os.ReadFile(diskPath)
		must(err)
	}
	writeFileMkdirMust(filepath.Join(dirAfter, relPath), after)
}

// delete directories older than 1 day in tempDir
func deleteOldDirs(tempDir string) {
	files, err := os.ReadDir(tempDir)
	must(err)
	for _, fi := range files {
		if !fi.IsDir() {
			// we shouldn't create anything but dirs
			continue
		}
		info, err := fi.Info()
		if err != nil {
			continue
		}
		age := time.Since(info.ModTime())
		path := filepath.Join(tempDir, fi.Name())
		if age > time.Hour*24 {
			fmt.Printf("Deleting %s because older than 1 day\n", path)
			err = os.RemoveAll(path)
			must(err)
		}
	}
}

// WinmergeDiffPreview shows uncommitted changes in the git repository
// of the current directory in WinMerge.
func WinmergeDiffPreview() {
	gitPath := detectExeMust("git")
	winMergePath := detectWinMergeMust()

	// we want a stable name so that we can clean up old junk
	tempDir := filepath.Join(os.TempDir(), "sum-diff-preview")
	must(os.MkdirAll(tempDir, 0755))
	fmt.Printf("temp dir: %s\n", tempDir)
	deleteOldDirs(tempDir)

	// git status returns paths relative to the root of the repository
	gitRoot := gitRootMust(gitPath)
	out, err := runCmdInDir(gitRoot, gitPath, "status", "--porcelain", "-z")
	must(err)
	changes, err := parseGitStatusZ(out, false)
	must(err)
	if len(changes) == 0 {
		fmt.Printf("No changes to preview!\n")
		return
	}
	fmt.Printf("%d change(s)\n", len(changes))

	subDir := time.Now().Format("2006-01-02_15_04_05")
	dir := filepath.Join(tempDir, subDir)
	dirBefore, dirAfter := getBeforeAfterDirs(dir)
	must(os.MkdirAll(dirBefore, 0755))
	must(os.MkdirAll(dirAfter, 0755))
	for _, change := range changes {
		copyChangeMust(gitPath, gitRoot, dir, change)
	}
	runWinMergeMust(winMergePath, dir)
}
