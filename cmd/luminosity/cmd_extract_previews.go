package main

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/aalpern/luminosity"
	"github.com/jawher/mow.cli"
	log "github.com/sirupsen/logrus"
)

func CmdExtractPreviews(app *cli.Cli) {
	app.Command("extract", "Extract cached previews from a catalog", func(cmd *cli.Cmd) {

		cmd.Spec = "[--output-dir] PATH"

		path := cmd.StringArg("PATH", "", "Catalog to extract previews from")
		outdir := cmd.StringOpt("o output-dir", "previews", "Directory to write extracted previews to")

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
			defer catalog.Close()

			// Ensure outdir exists and is a directory
			fi, err := os.Stat(*outdir)
			if err != nil && os.IsNotExist(err) {
				if err = os.MkdirAll(*outdir, 0755); err != nil {
					log.WithFields(log.Fields{
						"action": "mkdir",
						"status": "error",
						"outdir": *outdir,
						"error":  err,
					}).Error("Unable to create output directory")
					return
				}
			} else if err != nil {
				log.WithFields(log.Fields{
					"action": "stat",
					"status": "error",
					"outdir": *outdir,
					"error":  err,
				}).Error("Unable to stat outdir")
				return
			}

			if fi != nil && !fi.IsDir() {
				log.WithFields(log.Fields{
					"action": "stat",
					"status": "not_a_directory",
					"outdir": *outdir,
				}).Error("outdir exists but is not a directory")
				return
			}

			// Open the previews catalog
			previews, err := catalog.Previews()
			if err != nil {
				log.WithFields(log.Fields{
					"action": "previews",
					"status": "error",
				}).Error("Error opening previews catalog")
				return
			}
			defer previews.Close()

			// Process the photos
			var successCount, errorCount int
			catalog.ForEachPhoto(func(photo *luminosity.PhotoRecord) error {
				filename := photo.BaseName + ".jpg"
				preview, err := photo.GetPreview()
				if err != nil {
					log.WithFields(log.Fields{
						"action": "extract",
						"status": "error",
						"photo":  photo.BaseName,
						"error":  err,
					}).Warn("Error retrieving photo preview, skipping")
					errorCount++
					return nil
				} else {
					if err := ioutil.WriteFile(filepath.Join(*outdir, filename), preview, 0644); err != nil {
						log.WithFields(log.Fields{
							"action":   "write",
							"status":   "error",
							"filename": filename,
							"error":    err,
						}).Warn("Error writing preview file")
						return err
					}
					log.WithFields(log.Fields{
						"action":   "write",
						"status":   "ok",
						"filename": filename,
					}).Info("Wrote preview")
					successCount++
				}
				return nil
			})

			log.WithFields(log.Fields{
				"action":        "extract",
				"status":        "done",
				"success_count": successCount,
				"error_count":   errorCount,
			}).Info("Complete")
		}
	})
}
