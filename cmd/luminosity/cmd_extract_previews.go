package main

import (
	"github.com/aalpern/luminosity"
	"github.com/jawher/mow.cli"
	log "github.com/sirupsen/logrus"
)

func CmdExtractPreviews(app *cli.Cli) {
	app.Command("extract", "Extract cached previews from a catalog", func(cmd *cli.Cmd) {

		cmd.Spec = "[--output-dir] PATH"

		path := cmd.StringArg("PATH", "", "Catalog to extract previews from")
		_ = cmd.StringArg("o output-dir", "previews", "Directory to write extracted previews to")

		cmd.Action = func() {
			catalog, err := luminosity.OpenCatalog(*path)
			if err != nil {
				log.WithFields(log.Fields{
					"action":  "catalog_open",
					"catalog": *path,
					"error":   err,
				}).Error("Error opening catalog")
				return
			}

			// ensure outdir exists

			catalog.ForEachPhoto(func(photo *luminosity.PhotoRecord) error {
				return nil
			})
		}
	})
}
