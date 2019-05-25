package main

import (
	"github.com/aalpern/luminosity"
	"github.com/jawher/mow.cli"
	log "github.com/sirupsen/logrus"
)

func CmdSunburst(app *cli.Cli) {
	app.Command("sunburst", "Generate stats for rendering sunburst graphs", func(cmd *cli.Cmd) {

		cmd.Spec = "[--outfile] CATALOG"

		outfile := cmd.StringOpt("o outfile", "sunburst.json",
			"Path to output file")
		catalog := cmd.StringArg("CATALOG", "",
			"Catalog to process")

		cmd.Action = func() {
			cat, err := luminosity.OpenCatalog(*catalog)
			if err != nil {
				log.WithFields(log.Fields{
					"action":  "catalog_open",
					"catalog": catalog,
					"error":   err,
				}).Error("Error opening catalog, skipping.")
				return
			}

			data, err := cat.GetSunburstStats()
			if err != nil {
				log.WithFields(log.Fields{
					"action":  "sunburst_stats",
					"catalog": catalog,
					"error":   err,
				}).Error("Error getting sunburst stats")
				return
			}

			write(*outfile, data)
		}
	})
}
