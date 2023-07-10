package worker

import (
	"log"
	"time"
)

type Worker struct {
}

func (worker *Worker) Start() {
	for {

		// log.Println("Wor
		log.Println("Worker is running")
		time.Sleep(5 * time.Second)
	}
}
