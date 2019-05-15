package main

import (
	"github.com/jawher/mow.cli"
)

func CmdPurgeSidecars(app *cli.Cli) {
	app.Command("purge-sidecars", "Purge sidecar files", func(cmd *cli.Cmd) {
	})
}
