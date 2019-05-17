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

		cmd.Spec = "[--outfile] CATALOG..."
		outfile := cmd.StringOpt("o outfile", "stats.json", "Path to output file")
		catalogs := cmd.StringsArg("CATALOG", nil, "Catalogs to process")

		cmd.Action = func() {
			merged := luminosity.NewCatalog()
			for _, path := range *catalogs {
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
					continue
				}

				jsPath := strings.Replace(filepath.Base(path), ".lrcat", ".json", 1)
				write(jsPath, c)

				merged.Merge(c)
			}

			write(*outfile, merged)
		}
	})
}

func write(path string, data interface{}) {
	js, _ := json.MarshalIndent(data, "", "  ")
	ioutil.WriteFile(path, js, 0644)
}
