package u

import (
	"fmt"
	"log"
	"os/exec"
	"runtime"
	"strings"
)

// from https://gist.github.com/hyg/9c4afcd91fe24316cbf0
func OpenBrowser(url string) {
	var err error

	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", url).Start()
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		err = exec.Command("open", url).Start()
	default:
		err = fmt.Errorf("unsupported platform")
	}
	if err != nil {
		log.Fatal(err)
	}
}

// FormatSize formats a number in a human-readable form e.g. 1.24 kB
func FormatSize(n int64) string {
	sizes := []int64{1024 * 1024 * 1024, 1024 * 1024, 1024}
	suffixes := []string{"GB", "MB", "kB"}
	for i, size := range sizes {
		if n >= size {
			s := fmt.Sprintf("%.2f", float64(n)/float64(size))
			return strings.TrimSuffix(s, ".00") + " " + suffixes[i]
		}
	}
	return fmt.Sprintf("%d bytes", n)
}

// Percent returns how many percent of total is sub
// 100% means total == sub, 50% means sub = total / 2
func Percent(total, sub int64) float64 {
	return float64(sub) * 100 / float64(total)
}
