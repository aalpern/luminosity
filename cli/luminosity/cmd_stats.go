package main

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/aalpern/luminosity"
	"github.com/jawher/mow.cli"
	log "github.com/sirupsen/logrus"
)

func CmdStats(app *cli.Cli) {
	app.Command("stats", "Generate catalog statistics", func(cmd *cli.Cmd) {

		cmd.Spec = "[--outfile] [--per-catalog] PATH..."

		outfile := cmd.StringOpt("o outfile", "stats.json",
			"Path to output file")
		perCatalog := cmd.BoolOpt("p per-catalog", false,
			"Output a summary .json file for each catalog, in addition to the merged output")
		paths := cmd.StringsArg("PATH", nil,
			"Paths to process, which can be .lrcat files or directories")

		cmd.Action = func() {
			merged := luminosity.NewCatalog()
			catalogPaths := luminosity.FindCatalogs((*paths)...)
			var total int

			for _, path := range catalogPaths {
				c, err := luminosity.OpenCatalog(path)
				if err != nil {
					log.WithFields(log.Fields{
						"action":  "catalog_open",
						"catalog": path,
						"error":   err,
					}).Warn("Error opening catalog, skipping")
					continue
				}

				err = c.Load()
				if err != nil {
					log.WithFields(log.Fields{
						"action":  "catalog_load",
						"catalog": path,
						"error":   err,
					}).Warn("Error loading catalog, skipping")
					c.Close()
					continue
				}

				if *perCatalog {
					jsPath := strings.Replace(filepath.Base(path), ".lrcat", ".json", 1)
					write(jsPath, c)
				}

				total++
				log.WithFields(log.Fields{
					"action": "process_catalog",
					"path":   path,
					"status": "ok",
				}).Info("Processed catalog")

				merged.Merge(c)
				c.Close()
			}

			write(*outfile, merged)

			log.WithFields(log.Fields{
				"action":             "status",
				"status":             "complete",
				"catalogs_processed": total,
			}).Info("Complete")
		}
	})
}

func write(path string, data interface{}) {
	log.WithFields(log.Fields{
		"action": "write",
		"file":   path,
	}).Debug("Writing JSON")
	js, _ := json.MarshalIndent(data, "", "  ")
	ioutil.WriteFile(path, js, 0644)
}
