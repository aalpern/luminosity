package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/aalpern/luminosity"
)

func main() {
	err := aggregate()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func aggregate() error {
	merged := luminosity.NewCatalog()
	for _, path := range os.Args[1:] {
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

	write("merged.json", merged)

	return nil
}

func write(path string, data interface{}) {
	js, _ := json.MarshalIndent(data, "", "  ")
	ioutil.WriteFile(path, js, 0644)
}
