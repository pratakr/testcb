package main

import (
	"os"

	"bitbucket.com/sdssc/paygate/api"
	"bitbucket.com/sdssc/paygate/worker"
)

func main() {
	runner := os.Getenv("RUNNER")
	api := api.Api{}
	worker := worker.Worker{}

	if runner == "api" {
		api.Start()
	} else if runner == "worker" {
		worker.Start()
	}
}
