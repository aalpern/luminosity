package main

import (
	"github.com/aalpern/luminosity"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func CmdSunburst() *cobra.Command {
	var outfile string
	var prettyPrint bool

	cmd := &cobra.Command{
		Use:   "sunburst [--outfile] [--pretty-print] CATALOG",
		Short: "Generate stats for rendering sunburst graphs",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			catalog := args[0]
			cat, err := luminosity.OpenCatalog(catalog)
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

			write(outfile, data, prettyPrint)
		},
	}

	cmd.Flags().StringVarP(&outfile, "outfile", "o", "sunburst.json",
		"Output file for sunburst chart JSON data")
	cmd.Flags().BoolVarP(&prettyPrint, "pretty-print", "p", false,
		"Format the JSON output indented for human readability")

	return cmd
}
