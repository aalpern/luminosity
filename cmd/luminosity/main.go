package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/jawher/mow.cli"
	log "github.com/sirupsen/logrus"
)

func main() {
	app := cli.App("luminosity", "Operate on Lightroom catalogs")

	app.Spec = "[--verbose]"

	verbose := app.BoolOpt("v verbose", false, "Enable debug logging")

	app.Before = func() {
		if *verbose {
			log.SetLevel(log.DebugLevel)
		}
	}

	CmdStats(app)
	CmdSidecars(app)
	CmdSunburst(app)
	CmdExtractPreviews(app)

	app.Run(os.Args)
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
