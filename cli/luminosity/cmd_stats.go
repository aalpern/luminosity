package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/aalpern/luminosity"
	"github.com/jawher/mow.cli"
	log "github.com/sirupsen/logrus"
)

const (
	CatalogExtension        = "lrcat"
	CatalogDataDirExtension = "lrdata"
)

func CmdStats(app *cli.Cli) {
	app.Command("stats", "Generate catalog statistics", func(cmd *cli.Cmd) {

		cmd.Spec = "[--outfile] [--per-catalog] [--recursive] PATH..."

		outfile := cmd.StringOpt("o outfile", "stats.json",
			"Path to output file")
		perCatalog := cmd.BoolOpt("p per-catalog", false,
			"Output a summary .json file for each catalog, in addition to the merged output")
		recursive := cmd.BoolOpt("R recursive", false,
			"Recurse into any directories include as PATH arguments")
		paths := cmd.StringsArg("PATH", nil,
			"Paths to process, which can be .lrcat files or directories")

		cmd.Action = func() {
			merged := luminosity.NewCatalog()
			catalogPaths := findCatalogs(*recursive, (*paths)...)
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

func findCatalogs(recurse bool, paths ...string) []string {
	found := make([]string, 0, len(paths))

	// For each path in the input
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			log.WithFields(log.Fields{
				"action": "find_catalogs",
				"status": "stat_error",
				"path":   path,
				"error":  "err",
			}).Warn("Cannot stat path")
			continue
		}

		// Process files
		if !info.IsDir() {
			if !strings.HasSuffix(path, CatalogExtension) {
				log.WithFields(log.Fields{
					"action": "find_catalogs",
					"status": "wrong_suffix",
					"path":   path,
				}).Debug("Not a catalog file")
			} else {
				found = append(found, path)
			}
		} else {
			// Process directories
			children := findCatalogsInDir(recurse, path)
			found = append(found, children...)
		}
	}
	return found
}

func findCatalogsInDir(recurse bool, path string) []string {
	found := make([]string, 0, 8)

	if recurse {
		filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
			if err != nil {
				log.WithFields(log.Fields{
					"action": "find_catalogs",
					"status": "walk_error",
					"path":   path,
					"error":  "err",
				}).Warn("Error walking path")
			} else if !info.IsDir() {
				found = append(found, findCatalogs(false, p)...)
			} else if info.IsDir() {
				// Skip the .lrdata directories which contain the
				// potentially huge number of cached image previews
				if strings.HasSuffix(p, CatalogDataDirExtension) {
					return filepath.SkipDir
				}
			}
			return nil
		})
	}

	return found
}

func write(path string, data interface{}) {
	log.WithFields(log.Fields{
		"action": "write",
		"file":   path,
	}).Debug("Writing JSON")
	js, _ := json.MarshalIndent(data, "", "  ")
	ioutil.WriteFile(path, js, 0644)
}
