package deploy

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/kjk/common/u"

	"github.com/melbahja/goph"
	"github.com/pkg/sftp"
)

const (
	kPermExecutable = 0755
)

type Config struct {
	// base config values
	ProjectName               string
	Domain                    string
	HTTPPort                  int
	FrontEndBuildDir          string
	ServerUser                string
	ServerIP                  string
	PrivateKeyPath            string
	CaddyConfigPath           string
	RebuildFrontEndMust       func(*Config)
	EmptyFrontEndBuildDirMust func(*Config)
	Logf                      func(format string, args ...any)

	// derived values (calculated by InitializeDeployConfig)
	TmuxSessionName      string
	ServerDir            string
	CaddyConfigDelim     string
	CaddyConfig          string
	SystemdRunScriptPath string
	SystemdRunScriptTmpl string
	SystemdService       string
	SystemdServicePath   string
	SystemdServiceLink   string
}

func InitializeDeployConfig(c *Config) {
	c.TmuxSessionName = c.ProjectName
	c.ServerDir = "/root/apps/" + c.ProjectName
	c.CaddyConfigDelim = "# ---- " + c.Domain
	c.CaddyConfig = fmt.Sprintf(`%s {
	reverse_proxy localhost:%v
}`, c.Domain, c.HTTPPort)

	c.SystemdRunScriptPath = path.Join(c.ServerDir, "systemd-run.sh")

	c.SystemdRunScriptTmpl = `#!/bin/bash
tmux new-session -d -s {sessionName}
tmux send-keys -t {sessionName} "cd {workdDir}" Enter
tmux send-keys -t {sessionName} "./{exeName} -run-prod" Enter
echo "finished running under tmux"
`

	c.SystemdService = fmt.Sprintf(`[Unit]
Description=%s
After=network.target

[Service]
ExecStart=%s
Type=oneshot
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
`, c.Domain, c.SystemdRunScriptPath)

	c.SystemdServicePath = path.Join(c.ServerDir, c.ProjectName+".service")
	c.SystemdServiceLink = fmt.Sprintf("/etc/systemd/system/%s.service", c.ProjectName)
}

var (
	panicIf = u.PanicIf
	logf    = logfSimple
)

func logfSimple(format string, args ...any) {
	fmt.Printf(format, args...)
}

func must(err error) {
	u.PanicIfErr(err, "unexpected error: %s\n", err)
}

func sftpFileNotExistsMust(sftp *sftp.Client, path string) {
	_, err := sftp.Stat(path)
	u.PanicIf(err == nil, "file '%s' already exists on the server\n", path)
}

func sftpMkdirAllMust(sftp *sftp.Client, path string) {
	err := sftp.MkdirAll(path)
	panicIf(err != nil, "sftp.MkdirAll('%s') failed with '%s'", path, err)
	logf("created '%s' dir on the server\n", path)
}

func sshRunCommandMust(client *goph.Client, exe string, args ...string) {
	cmd, err := client.Command(exe, args...)
	panicIf(err != nil, "client.Command() failed with '%s'\n", err)
	logf("running '%s' on the server\n", cmd.String())
	out, err := cmd.CombinedOutput()
	logf("%s:\n%s\n", cmd.String(), string(out))
	panicIf(err != nil, "cmd.Output() failed with '%s'\n", err)
}

func copyToServerMaybeGzippedMust(client *goph.Client, sftp *sftp.Client, localPath, remotePath string, gzipped bool) {
	if gzipped {
		remotePath += ".gz"
		sftpFileNotExistsMust(sftp, remotePath)
		u.GzipCompressFile(localPath+".gz", localPath)
		localPath += ".gz"
		defer os.Remove(localPath)
	}
	sizeStr := u.FormatSize(u.FileSize(localPath))
	logf("uploading '%s' (%s) to '%s'", localPath, sizeStr, remotePath)
	timeStart := time.Now()
	err := client.Upload(localPath, remotePath)
	panicIf(err != nil, "\nclient.Upload() failed with '%s'", err)
	logf(" took %s\n", time.Since(timeStart))

	if gzipped {
		// ungzip on the server
		sshRunCommandMust(client, "gzip", "-d", remotePath)
	}
}

func createNewTmuxSession(name string) {
	cmd := exec.Command("tmux", "new-session", "-d", "-s", name)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(out), "duplicate session") {
			logf("tmux session '%s' already exists\n", name)
			return
		}
		panicIf(err != nil, "tmux new-session failed with '%s'\n", err)
		logf("%s:\n%s\n", cmd.String(), string(out))
	}
}

func tmuxSendKeys(sessionName string, text string) {
	cmd := exec.Command("tmux", "send-keys", "-t", sessionName, text, "Enter")
	out, err := cmd.CombinedOutput()
	logf("%s:\n%s\n", cmd.String(), string(out))
	panicIf(err != nil, "%s failed with %s\n", cmd.String(), err)
}

func deleteOldBuilds(c *Config) {
	pattern := c.ProjectName + "-*"
	files, err := filepath.Glob(pattern)
	must(err)
	for _, path := range files {
		err = os.Remove(path)
		must(err)
		logf("deleted %s\n", path)
	}
}

func writeToFileMust(path string, content string, perm os.FileMode) {
	logf("createEmptyFile: '%s'\n", path)
	must(os.MkdirAll(filepath.Dir(path), 0755))
	os.Remove(path)
	err := os.WriteFile(path, []byte(content), perm)
	must(err)
}

func buildForProd(c *Config, forLinux bool) string {
	// re-build the frontend. remove build process artifacts
	// to keep things clean

	c.RebuildFrontEndMust(c)
	defer func() {
		c.EmptyFrontEndBuildDirMust(c)
	}()

	hashShort, date := u.GetGitHashDateMust()
	exeName := fmt.Sprintf("%s-%s-%s", c.ProjectName, date, hashShort)
	if u.IsWindows() && !forLinux {
		exeName += ".exe"
	}

	// build the binary, for linux if forLinux is true, otherwise for OS arh
	{
		ldFlags := "-X main.GitCommitHash=" + hashShort
		cmd := exec.Command("go", "build", "-o", exeName, "-ldflags", ldFlags, ".")
		if forLinux {
			cmd.Env = os.Environ()
			cmd.Env = append(cmd.Env, "GOOS=linux", "GOARCH=amd64")
		}
		out, err := cmd.CombinedOutput()
		logf("%s:\n%s\n", cmd.String(), out)
		panicIf(err != nil, "go build failed")

		sizeStr := u.FormatSize(u.FileSize(exeName))
		logf("created '%s' of size %s\n", exeName, sizeStr)
	}

	return exeName
}

/*
How deploying to hetzner works:
- compile linux binary with name ${app}-YYYY-MM-DD-${hashShort}
- copy binary to hetzner
- run on hetzner
*/
func ToHetzner(c *Config) {
	if c.Logf != nil {
		logf = c.Logf
	}
	deleteOldBuilds(c)
	exeName := buildForProd(c, true)
	panicIf(!u.FileExists(exeName), "file '%s' doesn't exist", exeName)

	serverExePath := path.Join(c.ServerDir, exeName)

	keyPath := u.ExpandTildeInPath(c.PrivateKeyPath)
	panicIf(!u.FileExists(keyPath), "key file '%s' doesn't exist", keyPath)
	auth, err := goph.Key(keyPath, "")
	panicIf(err != nil, "goph.Key() failed with '%s'", err)
	client, err := goph.New(c.ServerUser, c.ServerIP, auth)
	panicIf(err != nil, "goph.New() failed with '%s'", err)
	defer client.Close()

	// global sftp client for multiple operations
	sftp, err := client.NewSftp()
	panicIf(err != nil, "client.NewSftp() failed with '%s'", err)
	defer sftp.Close()

	// check:
	// - caddy is installed
	// - binary doesn't already exists
	{
		_, err = sftp.Stat(c.CaddyConfigPath)
		panicIf(err != nil, "sftp.Stat() for '%s' failed with '%s'\nInstall caddy on the server?\n", c.CaddyConfigPath, err)

		sftpFileNotExistsMust(sftp, serverExePath)
	}

	// create destination dir on the server
	sftpMkdirAllMust(sftp, c.ServerDir)

	// copy binary to the server
	copyToServerMaybeGzippedMust(client, sftp, exeName, serverExePath, true)

	// make the file executable
	{
		err = sftp.Chmod(serverExePath, 0755)
		panicIf(err != nil, "sftp.Chmod() failed with '%s'", err)
		logf("created dir on the server '%s'\n", c.ServerDir)
	}

	sshRunCommandMust(client, serverExePath, "-setup-and-run")
	logf("Running on http://%s:%d or https://%s\n", c.ServerIP, c.HTTPPort, c.Domain)
}

func SetupOnServerAndRun(c *Config) {
	logf("SetupOnServerAndRun: projectName: '%s'\n", c.ProjectName)

	if !u.FileExists(c.CaddyConfigPath) {
		logf("%s doesn't exist.\nMust install caddy?\n", c.CaddyConfigPath)
		os.Exit(1)
	}

	// kill existing process
	// note: muse use "ps ax" (and not e.g. "pkill") because we don't want to kill ourselves
	{
		out := u.RunMust("ps", "ax")
		lines := strings.Split(out, "\n")
		pidsToKill := []string{}
		for _, l := range lines {
			if len(l) == 0 {
				continue
			}
			parts := strings.Fields(l)
			//parts := strings.SplitN(l, "\t", 5)
			if len(parts) < 5 {
				logf("unexpected line in ps ax: '%s', len(parts)=%d\n", l, len(parts))
				continue
			}
			pid := parts[0]
			name := parts[4]
			if !strings.Contains(name, c.ProjectName) {
				//logf("skipping process '%s' pid: '%s'\n", name, pid)
				continue
			}
			logf("MAYBE KILLING process '%s' pid: '%s'\n", name, pid)
			myPid := fmt.Sprintf("%v", os.Getpid())
			if pid == myPid {
				logf("NOT KILLING because it's myself\n")
				// no suicide allowed
				continue
			}
			pidsToKill = append(pidsToKill, pid)
			logf("found process to kill: '%s' pid: '%s'\n", name, pid)
		}
		for _, pid := range pidsToKill {
			u.RunLoggedMust("kill", pid)
		}
		if len(pidsToKill) == 0 {
			logf("no %s* processes to kill\n", c.ProjectName)
		}
	}

	ownExeName := filepath.Base(os.Args[0])
	if false {
		createNewTmuxSession(c.TmuxSessionName)
		// cd to deployServer
		tmuxSendKeys(c.TmuxSessionName, fmt.Sprintf("cd %s", c.ServerDir))
		// run the server
		tmuxSendKeys(c.TmuxSessionName, fmt.Sprintf("./%s -run-prod", ownExeName))
	}

	// configure systemd to restart on reboot
	{
		// systemd-run.sh script that will be called by systemd on reboot
		runScript := strings.ReplaceAll(c.SystemdRunScriptTmpl, "{exeName}", ownExeName)
		runScript = strings.ReplaceAll(runScript, "{sessionName}", c.ProjectName)
		runScript = strings.ReplaceAll(runScript, "{workdDir}", c.ServerDir)
		writeToFileMust(c.SystemdRunScriptPath, runScript, kPermExecutable)

		// systemd .service file linked from /etc/systemd/system/
		writeToFileMust(c.SystemdServicePath, c.SystemdService, kPermExecutable)
		os.Remove(c.SystemdServiceLink)
		err := os.Symlink(c.SystemdServicePath, c.SystemdServiceLink)
		panicIf(err != nil, "os.Symlink(%s, %s) failed with '%s'", c.SystemdServicePath,
			c.SystemdServiceLink, err)
		logf("created symlink '%s' to '%s'\n", c.SystemdServiceLink, c.SystemdServicePath)

		serviceName := c.ProjectName + ".service"

		// daemon-reload needed if service file changed
		u.RunLoggedMust("systemctl", "daemon-reload")
		// runLoggedMust("systemctl", "start", serviceName)
		u.RunLoggedMust("systemctl", "enable", serviceName)

		u.RunLoggedMust(c.SystemdRunScriptPath)
	}

	// update and reload caddy config
	didReplace := u.AppendOrReplaceInFileMust(c.CaddyConfigPath, c.CaddyConfig, c.CaddyConfigDelim)
	if didReplace {
		u.RunLoggedMust("systemctl", "reload", "caddy")
	}

	// archive previous deploys
	{
		pattern := filepath.Join(c.ServerDir, c.ProjectName+"-*")
		files, err := filepath.Glob(pattern)
		must(err)
		logf("archiving previous deploys, pattern: '%s', %d files\n", pattern, len(files))
		backupDir := filepath.Join(c.ServerDir, "backup")
		for _, file := range files {
			name := filepath.Base(file)
			if name == ownExeName {
				logf("skipping archiving of '%s' (myself)\n", name)
				continue
			}
			backupPath := filepath.Join(backupDir, name)
			err = os.MkdirAll(backupDir, 0755)
			u.PanicIfErr(err, "os.MkdirAll('%s') failed with %s\n", backupDir, err)
			err = os.Rename(file, backupPath)
			u.PanicIfErr(err, "os.Rename('%s', '%s') failed with %s\n", file, backupPath, err)
			logf("moved '%s' to '%s'\n", file, backupPath)
		}
	}
	// delete archived builds, leave 5 most recent
	{
		pattern := filepath.Join(c.ServerDir, "backup", c.ProjectName+"-*")
		files, err := filepath.Glob(pattern)
		must(err)
		logf("deleting old archived builds, pattern: '%s', %d files\n", pattern, len(files))
		slices.Sort(files)
		slices.Reverse(files)
		for i, file := range files {
			if i < 5 {
				logf("skipping deletion of '%s'\n", file)
				continue
			}
			err = os.Remove(file)
			u.PanicIfErr(err, "os.Remove('%s') failed with %s\n", file, err)
			logf("deleted '%s'\n", file)
		}
	}
}
