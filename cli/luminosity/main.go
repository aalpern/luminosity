package main

import (
	"os"

	"github.com/jawher/mow.cli"
	log "github.com/sirupsen/logrus"
)

func main() {
	app := cli.App("luminosity", "Operate on Lightroom catalogs")

	app.Spec = "[--verbose]"

	verbose := app.BoolOpt("v verbose", false, "Enable debug logging")

	app.Before = func() {
		if *verbose {
			log.SetLevel(log.DebugLevel)
		}
	}

	CmdStats(app)
	CmdSidecars(app)
	CmdSunburst(app)

	app.Run(os.Args)
}
