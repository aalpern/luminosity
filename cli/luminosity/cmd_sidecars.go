package main

import (
	"fmt"
	"os"

	"github.com/aalpern/luminosity"
	"github.com/cloudfoundry/bytefmt"
	"github.com/jawher/mow.cli"
)

func CmdSidecars(app *cli.Cli) {
	app.Command("sidecars", "Purge sidecar files", func(cmd *cli.Cmd) {

		cmd.Spec = "[--list|--delete|--summary] CATALOG..."

		doList := cmd.BoolOpt("l list", false, "Output a list of sidecar files")
		doSummary := cmd.BoolOpt("s summary", false, "Output summary info about sidecar files")
		doDelete := cmd.BoolOpt("d delete", false, "Delete sidecar files on disk")
		catalogs := cmd.StringsArg("CATALOG", nil, "Catalogs to process")

		cmd.Action = func() {
			for _, path := range *catalogs {
				catalog, err := luminosity.OpenCatalog(path)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error opening catalog %s; %s. Catalog will be ignored.\n",
						path, err)
					continue
				}

				if *doSummary {
					info, err := catalog.GetSidecarFileStats()
					if err != nil {
						// TODO: use a logging package
					} else {
						fmt.Printf("Sidecar Summary for %s\n", path)
						fmt.Printf("  Count:             %d\n", info.Count)
						fmt.Printf("  Total Size:        %s\n",
							bytefmt.ByteSize(uint64(info.TotalSizeBytes)))
						fmt.Printf("  Missing Sidecars:  %d\n", info.MissingSidecarCount)
						fmt.Printf("  Missing Originals: %d\n", info.MissingOriginalCount)
					}
				} else if *doList {
					catalog.ForEachSidecar(func(rec *luminosity.SidecarFileRecord) error {
						fmt.Printf("%s\n", rec.SidecarPath)
						return nil
					})
				} else if *doDelete {
					var processed, errors, skipped, missing, total uint
					catalog.ForEachSidecar(func(rec *luminosity.SidecarFileRecord) error {
						if _, err := os.Stat(rec.SidecarPath); err == nil {
							if _, err := os.Stat(rec.OriginalPath); os.IsNotExist(err) {
								fmt.Printf("Missing original path for sidecar; Skipping %s\n", rec.SidecarPath)
								skipped++
								return nil
							}

							fmt.Printf("Deleting %s\n", rec.SidecarPath)
							err = os.Remove(rec.SidecarPath)
							if err != nil {
								fmt.Printf("Error deleting %s; %v.\n", rec.SidecarPath, err)
								errors++
								return err
							} else {
								processed++
							}
						} else {
							fmt.Printf("Missing %s, skipping\n", rec.SidecarPath)
							missing++
						}
						total++
						return nil
					})
					fmt.Printf("Done.\n")
					fmt.Printf("   Total:   %d\n", total)
					fmt.Printf("   Deleted: %d\n", processed)
					fmt.Printf("   Skipped: %d\n", skipped)
					fmt.Printf("   Missing: %d\n", missing)
				}
			}
		}
	})
}
