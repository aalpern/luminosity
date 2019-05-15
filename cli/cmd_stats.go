package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/aalpern/luminosity"
	"github.com/jawher/mow.cli"
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
					fmt.Fprintf(os.Stderr, "Error opening catalog %s; %s. Catalog will be ignored.\n",
						path, err)
					continue
				}
				err = c.Load()
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error loading catalog %s; %s. Catalog will be ignored.\n",
						path, err)
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
