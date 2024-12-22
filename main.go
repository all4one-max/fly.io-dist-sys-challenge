package main

import (
    "log"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
    "maelstrom-echo/handlers"
)

func main() {
    n := maelstrom.NewNode()

    // Register handlers for different challenges
    handlers.RegisterGenerateHandler(n)
	// handlers.RegisterSingleNodeBroadcastHandler(n)
    handlers.RegisterMultiNodeBroadcastHandler(n)

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }

}