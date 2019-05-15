package main

import (
	"os"

	"github.com/jawher/mow.cli"
)

func main() {
	app := cli.App("luminosity", "Operate on Lightroom catalogs")

	CmdStats(app)
	CmdPurgeSidecars(app)

	app.Run(os.Args)
}
