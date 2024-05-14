package u

import (
	"fmt"
	"io"
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

var (
	gitPath      string
	winMergePath string
	tempDir      string
)

const (
	// gitStatusModified represents a modified git status line
	gitStatusModified = iota
	// gitSatusAdded represents a added git status line
	gitSatusAdded
	// gitStatusDeleted represents a deleted git status line
	gitStatusDeleted
	// gitStatusNotCheckedIn represents a not checked in git status line
	gitStatusNotCheckedIn
)

// gitChange represents a single git change
type gitChange struct {
	Type int // Modified, Added etc.
	Path string
	Name string
}

func detectExeMust(name string) string {
	path, err := exec.LookPath(name)
	if err == nil {
		fmt.Printf("'%s' is '%s'\n", name, path)
		return path
	}
	fmt.Printf("Couldn't find '%s'\n", name)
	must(err)
	// TODO: could also try known locations for WinMerge in $env["ProgramFiles(x86)"]/WinMerge/WinMergeU.exe
	return ""
}

func parseGitStatusLineMust(s string) *gitChange {
	c := &gitChange{}
	parts := strings.SplitN(s, " ", 2)
	PanicIf(len(parts) != 2, "invalid line: '%s'\n", s)
	switch parts[0] {
	case "M":
		c.Type = gitStatusModified
	case "A":
		c.Type = gitSatusAdded
	case "D":
		c.Type = gitStatusDeleted
	case "??":
		c.Type = gitStatusNotCheckedIn
	case "RM":
		// TODO: handle line:
		// RM tools/diff-preview.go -> do/diff_preview.go
		return nil
	default:
		PanicIf(true, "invalid line: '%s'\n", s)
	}
	c.Path = strings.TrimSpace(parts[1])
	c.Name = filepath.Base(c.Path)
	return c
}

func detectExesMust() {
	gitPath = detectExeMust("git")
	path := `C:\Program Files\WinMerge\WinMergeU.exe`
	if !PathExists(path) {
		path = `C:\Users\kjk\AppData\Local\Programs\WinMerge\WinMergeU.exe`
	}
	if PathExists(path) {
		winMergePath = path
		return
	}
	winMergePath = detectExeMust("WinMergeU")
}

func createTempDirMust() {
	dir := getWinTempDirMust()
	// we want a stable name so that we can clean up old junk
	tempDir = filepath.Join(dir, "sum-diff-preview")
	err := os.MkdirAll(tempDir, 0755)
	must(err)
}

func getWinTempDirMust() string {
	dir := os.Getenv("TEMP")
	if dir != "" {
		return dir
	}
	dir = os.Getenv("TMP")
	PanicIf(dir == "", "env variable TEMP and TMP are not set\n")
	return dir
}

func runCmd(exePath string, args ...string) ([]byte, error) {
	cmd := exec.Command(exePath, args...)
	fmt.Printf("running: %s %v\n", filepath.Base(exePath), args)
	return cmd.Output()
}

func runCmdNoWait(exePath string, args ...string) error {
	cmd := exec.Command(exePath, args...)
	fmt.Printf("running: %s %v\n", filepath.Base(exePath), args)
	return cmd.Start()
}

func parseGitStatusMust(out []byte, includeNotCheckedIn bool) []*gitChange {
	var res []*gitChange
	lines := ToTrimmedLines(out)
	for _, l := range lines {
		c := parseGitStatusLineMust(l)
		if c == nil {
			continue
		}
		if !includeNotCheckedIn && c.Type == gitStatusNotCheckedIn {
			continue
		}
		res = append(res, c)
	}
	return res
}

func gitStatusMust() []*gitChange {
	out, err := runCmd(gitPath, "status", "--porcelain")
	must(err)
	return parseGitStatusMust(out, false)
}

func gitGetFileContentHeadMust(path string) []byte {
	loc := "HEAD:" + path
	out, err := runCmd(gitPath, "show", loc)
	must(err)
	return out
}

func getBeforeAfterDirs(dir string) (string, string) {
	dirBefore := filepath.Join(dir, "before")
	dirAfter := filepath.Join(dir, "after")
	return dirBefore, dirAfter
}

// https://manual.winmerge.org/Command_line.html
func runWinMerge(dir string) {
	dirBefore, dirAfter := getBeforeAfterDirs(dir)
	/*
		/e : close with Esc
		/u : don't add paths to MRU
		/wl, wr : open left/right as read-only
		/r : recursive compare
	*/
	err := runCmdNoWait(winMergePath, "/u", "/wl", "/wr", dirBefore, dirAfter)
	must(err)
}

func catGitHeadToFileMust(dst, gitPath string) {
	fmt.Printf("catGitHeadToFileMust: %s => %s\n", gitPath, dst)
	d := gitGetFileContentHeadMust(gitPath)
	f, err := os.Create(dst)
	must(err)
	defer f.Close()
	_, err = f.Write(d)
	must(err)
}

func createEmptyFileMust(path string) {
	f, err := os.Create(path)
	must(err)
	f.Close()
}

func copyFileMust(dst, src string) {
	// ensure windows-style dir separator
	dst = strings.Replace(dst, "/", "\\", -1)
	src = strings.Replace(src, "/", "\\", -1)

	fdst, err := os.Create(dst)
	must(err)
	defer fdst.Close()
	fsrc, err := os.Open(src)
	must(err)
	defer fsrc.Close()
	_, err = io.Copy(fdst, fsrc)
	must(err)
}
func copyFileAddedMust(dirBefore, dirAfter string, change *gitChange) {
	// empty file in before, content in after
	path := filepath.Join(dirBefore, change.Name)
	createEmptyFileMust(path)
	path = filepath.Join(dirAfter, change.Name)
	copyFileMust(path, change.Path)
}

func copyFileDeletedMust(dirBefore, dirAfter string, change *gitChange) {
	// empty file in after
	path := filepath.Join(dirAfter, change.Name)
	createEmptyFileMust(path)
	// version from HEAD in before
	path = filepath.Join(dirBefore, change.Name)
	catGitHeadToFileMust(path, change.Path)
}

func copyFileModifiedMust(dirBefore, dirAfter string, change *gitChange) {
	// current version on disk in after
	path := filepath.Join(dirAfter, change.Name)
	copyFileMust(path, change.Path)
	// version from HEAD in before
	path = filepath.Join(dirBefore, change.Name)
	catGitHeadToFileMust(path, change.Path)
}

func copyFileChangeMust(dir string, change *gitChange) {
	dirBefore, dirAfter := getBeforeAfterDirs(dir)
	switch change.Type {
	case gitSatusAdded:
		copyFileAddedMust(dirBefore, dirAfter, change)
	case gitStatusModified:
		copyFileModifiedMust(dirBefore, dirAfter, change)
	case gitStatusDeleted:
		copyFileDeletedMust(dirBefore, dirAfter, change)
	default:
		PanicIf(true, "unknown change %+v\n", change)
	}
}

func gitCopyFiles(dir string, changes []*gitChange) {
	dirBefore, dirAfter := getBeforeAfterDirs(dir)
	err := os.MkdirAll(dirBefore, 0755)
	must(err)
	err = os.MkdirAll(dirAfter, 0755)
	must(err)
	for _, change := range changes {
		copyFileChangeMust(dir, change)
	}
}

// delete directories older than 1 day in tempDir
func deleteOldDirs() {
	files, err := os.ReadDir(tempDir)
	must(err)
	for _, fi := range files {
		if !fi.IsDir() {
			// we shouldn't create anything but dirs
			continue
		}
		info, _ := fi.Info()
		age := time.Since(info.ModTime())
		path := filepath.Join(tempDir, fi.Name())
		if age > time.Hour*24 {
			fmt.Printf("Deleting %s because older than 1 day\n", path)
			err = os.RemoveAll(path)
			must(err)
		} else {
			fmt.Printf("Not deleting %s because younger than 1 day (%s)\n", path, age)
		}
	}
}

func hasGitDirMust(dir string) bool {
	files, err := os.ReadDir(dir)
	must(err)
	for _, fi := range files {
		if strings.ToLower(fi.Name()) == ".git" {
			return fi.IsDir()
		}
	}
	return false
}

// git status returns names relative to root of
func cdToGitRoot() {
	var newDir string
	dir, err := os.Getwd()
	must(err)
	for {
		if hasGitDirMust(dir) {
			break
		}
		newDir = filepath.Dir(dir)
		PanicIf(dir == newDir, "dir == newDir (%s == %s)", dir, newDir)
		dir = newDir
	}
	if newDir != "" {
		fmt.Printf("Changed current dir to: '%s'\n", newDir)
		os.Chdir(newDir)
	}
}

func WinmergeDiffPreview() {
	detectExesMust()
	createTempDirMust()
	fmt.Printf("temp dir: %s\n", tempDir)
	deleteOldDirs()

	cdToGitRoot()
	changes := gitStatusMust()
	if len(changes) == 0 {
		fmt.Printf("No changes to preview!")
		os.Exit(0)
	}
	fmt.Printf("%d change(s)\n", len(changes))

	// TODO: verify GitChange.Name is unique in changes
	subDir := time.Now().Format("2006-01-02_15_04_05")
	dir := filepath.Join(tempDir, subDir)
	err := os.MkdirAll(dir, 0755)
	must(err)
	gitCopyFiles(dir, changes)
	runWinMerge(dir)
}
