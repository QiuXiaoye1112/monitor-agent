package main

import (
	"os"

	"monitor-agent/cmd"
)

func main() {
	cmd.Execute()
	os.Exit(0)
}
