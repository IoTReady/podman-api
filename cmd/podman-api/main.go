// Command podman-api is the HTTP service that translates CMS REST calls
// into libpod REST calls against one or more Podman hosts.
package main

import (
	"log"

	"github.com/iotready/podman-api/server"
)

func main() {
	if err := server.RunWithFlags(); err != nil {
		log.Fatal(err)
	}
}
