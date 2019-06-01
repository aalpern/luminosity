package main

import (
	"fmt"
	"os"

	"github.com/aalpern/luminosity"
	"github.com/cloudfoundry/bytefmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func CmdSidecars() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sidecars",
		Short: "Operate on sidecar files",
	}

	cmd.AddCommand(
		sidecarsSummary(),
		sidecarsList(),
		sidecarsDelete())

	return cmd
}

func sidecarsSummary() *cobra.Command {
	return &cobra.Command{
		Use:   "summary CATALOG...",
		Short: "List sidecar stats",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			for _, path := range args {
				catalog, err := luminosity.OpenCatalog(path)
				if err != nil {
					log.WithFields(log.Fields{
						"action":  "catalog_open",
						"catalog": path,
						"error":   err,
					}).Warn("Error opening catalog, skipping.")
					continue
				}

				info, err := catalog.GetSidecarFileStats()
				if err != nil {
					log.WithFields(log.Fields{
						"action":  "sidecar_stats",
						"catalog": path,
						"error":   err,
					}).Error("Error getting sidecar file stats")
				} else {
					fmt.Printf("Sidecar Summary for %s\n", path)
					fmt.Printf("  Count:             %d\n", info.Count)
					fmt.Printf("  Total Size:        %s\n",
						bytefmt.ByteSize(uint64(info.TotalSizeBytes)))
					fmt.Printf("  Missing Sidecars:  %d\n", info.MissingSidecarCount)
					fmt.Printf("  Missing Originals: %d\n", info.MissingOriginalCount)
				}
			}
		},
	}
}

//		cmd.Command("delete", "Delete all sidecar files", sidecarsDelete)

func sidecarsList() *cobra.Command {
	return &cobra.Command{
		Use:   "list CATALOG...",
		Short: "List all sidecar file paths",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			for _, path := range args {
				catalog, err := luminosity.OpenCatalog(path)
				if err != nil {
					log.WithFields(log.Fields{
						"action":  "catalog_open",
						"catalog": path,
						"error":   err,
					}).Warn("Error opening catalog, skipping.")
					continue
				}

				catalog.ForEachSidecar(func(rec *luminosity.SidecarFileRecord) error {
					fmt.Printf("%s\n", rec.SidecarPath)
					return nil
				})
			}
		},
	}
}

func sidecarsDelete() *cobra.Command {
	var deleteMissingOriginals bool

	cmd := &cobra.Command{
		Use:   "delete CATALOG...",
		Short: "Delete all sidecar files",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			for _, path := range args {
				catalog, err := luminosity.OpenCatalog(path)
				if err != nil {
					log.WithFields(log.Fields{
						"action":  "catalog_open",
						"catalog": path,
						"error":   err,
					}).Warn("Error opening catalog, skipping.")
					continue
				}

				var processed, errors, skipped, missing, total uint
				catalog.ForEachSidecar(func(rec *luminosity.SidecarFileRecord) error {
					if _, err := os.Stat(rec.SidecarPath); err == nil {
						if _, err := os.Stat(rec.OriginalPath); os.IsNotExist(err) {
							if deleteMissingOriginals {
								log.WithFields(log.Fields{
									"action": "delete",
									"status": "missing_original",
									"path":   rec.SidecarPath,
								}).Warn("Missing original path for sidecar; Deleting")

							} else {
								log.WithFields(log.Fields{
									"action": "delete",
									"path":   rec.SidecarPath,
								}).Info("Missing original path for sidecar; Skipping")
								skipped++
								return nil
							}
						}

						err = os.Remove(rec.SidecarPath)
						if err != nil {
							log.WithFields(log.Fields{
								"action": "delete",
								"status": "error",
								"path":   rec.SidecarPath,
								"error":  err,
							}).Error("Error deleting sidecar")
							errors++
							return err
						} else {
							processed++
						}
					} else {
						log.WithFields(log.Fields{
							"action": "delete",
							"status": "skip",
							"path":   rec.SidecarPath,
						}).Error("Missing sidecar; Skipping")
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
		},
	}
	cmd.Flags().BoolVarP(&deleteMissingOriginals, "delete-missing-originals", "", false,
		"Delete sidecar even if the original is missing")
	return cmd
}
