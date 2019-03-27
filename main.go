package main

import (
	"github.com/tsaikd/gogstash/cmd"
	"math/rand"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	cmd.Module.MustMainRun()
}
