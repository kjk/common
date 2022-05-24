package server

import (
	"math/rand"
	"net/http"
	"strings"
	"time"
)

/*
probably doesn't matter, but when somone is scanning our website
for vulnerabilities, send them 200 response with random data.
let them choke on it.
the urls come from observing attacks on my websites
*/

var (
	// exact url matche
	badClients = map[string]bool{
		"/images/":                   true,
		"/files/":                    true,
		"/uploads/":                  true,
		"/sites/default/files/":      true,
		"/templates/beez3/ALFA_DATA": true,
	}
	// if url contains
	badClientsContains = []string{
		"/wp-login.php",
		"/wp-includes/wlwmanifest.xml",
		"/xmlrpc.php",
		".env",
		"id_rsa",
		"id_dsa",
		"/etc/passwd",
	}
	// if url starts with
	badClientPrefix = []string{
		"/wp-", // lots
		"/.well-known/",
		"/plus/",
		"/?-",
		"/index?-",
		"/.git",
		"/admin",
	}
	// if url ends with
	badClientSuffix = []string{
		".bak",
		".sql",
		".key",
		".pem",
		".sqlite",
		".db",
	}
	badClientsRandomData []byte
)

func init() {
	// generate random data to send. It starts with valid html
	// to trick parser into reading them (and hopefully choking
	// when parsing the remaining random binary data)
	// TODO: could re-generate them every N requests
	// (doing it on every requests would be unnecessarily expensive)
	d := make([]byte, 0, 1024)
	d = append(d, []byte("<html><body>fuck you...")...)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	start := len(d)
	for i := start; i < 1024; i++ {
		d = append(d, byte(rnd.Intn(256)))
	}
	badClientsRandomData = d
}

// returns true if sent a response to the client
func TryServeBadClient(w http.ResponseWriter, r *http.Request, isBadURL func(s string) bool) bool {
	isBad := func(uri string) bool {
		if badClients[uri] {
			return true
		}
		for _, s := range badClientSuffix {
			if strings.HasSuffix(uri, s) {
				return true
			}
		}
		for _, s := range badClientPrefix {
			if strings.HasPrefix(uri, s) {
				return true
			}
		}
		for _, s := range badClientsContains {
			if strings.Contains(uri, s) {
				return true
			}
		}
		if isBadURL != nil {
			return isBadURL(uri)
		}
		return false
	}
	if !isBad(r.URL.Path) {
		return false
	}
	w.Header().Add("Content-Tyep", "text/html")
	w.WriteHeader(200)
	w.Write(badClientsRandomData)
	return true
}
