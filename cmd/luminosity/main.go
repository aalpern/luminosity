package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/spf13/cobra"

	log "github.com/sirupsen/logrus"
)

func main() {
	var verbose bool

	cmd := &cobra.Command{
		Use:   "luminosity [--verbose]",
		Short: "Operate on Lightroom catalogs",
		Long: `luminosity is a CLI tool for the luminosity library,
providing commands to performance various operations 
on Adobe Lightroom catalogs, such as generating 
analytics data for usage reports, extracting previews,
and managing sidecars.`,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if verbose {
				log.SetLevel(log.DebugLevel)
			}
		},
	}

	cmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable debug logging")

	cmd.AddCommand(
		CmdSunburst(),
		CmdStats(),
		CmdSidecars(),
		CmdExtractPreviews())

	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func write(path string, data interface{}, prettyPrint bool) {
	log.WithFields(log.Fields{
		"action": "write",
		"file":   path,
	}).Debug("Writing JSON")
	var js []byte
	if prettyPrint {
		js, _ = json.MarshalIndent(data, "", "  ")
	} else {
		js, _ = json.Marshal(data)
	}
	ioutil.WriteFile(path, js, 0644)
}

func dump(data interface{}, prettyPrint bool) {
	var js []byte
	if prettyPrint {
		js, _ = json.MarshalIndent(data, "", "  ")
	} else {
		js, _ = json.Marshal(data)
	}
	fmt.Println(string(js))
}
