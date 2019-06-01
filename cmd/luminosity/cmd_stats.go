package main

import (
	"path/filepath"
	"strings"

	"github.com/aalpern/luminosity"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func CmdStats() *cobra.Command {
	var outfile string
	var perCatalog bool
	var prettyPrint bool

	cmd := &cobra.Command{
		Use:   "stats PATH...",
		Short: "Generate catalog statistics",
		Args:  cobra.MinimumNArgs(1),
	}

	cmd.Flags().StringVarP(&outfile, "outfile", "o", "stats.json",
		"Path to output file")
	cmd.Flags().BoolVarP(&perCatalog, "per-catalog", "c", false,
		"Output a summary .json file for each catalog, in addition to the merged output")
	cmd.Flags().BoolVarP(&prettyPrint, "pretty-print", "p", false,
		"Format the JSON output indented for human readability")

	// paths := cmd.StringsArg("PATH", nil,
	// "Paths to process, which can be .lrcat files or directories")

	cmd.Run = func(cmd *cobra.Command, args []string) {
		merged := luminosity.NewCatalog()
		catalogPaths := luminosity.FindCatalogs((args)...)
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

			if perCatalog {
				jsPath := strings.Replace(filepath.Base(path), ".lrcat", ".json", 1)
				write(jsPath, c, prettyPrint)
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

		write(outfile, merged, prettyPrint)

		log.WithFields(log.Fields{
			"action":             "status",
			"status":             "complete",
			"catalogs_processed": total,
		}).Info("Complete")
	}

	return cmd
}
