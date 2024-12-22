package handlers

import (
    "fmt"
    "sync/atomic"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Counter for atomic operations
var counter uint64

// RegisterGenerateHandler registers the handler for "generate" messages
func RegisterGenerateHandler(n *maelstrom.Node) {
    n.Handle("generate", func(msg maelstrom.Message) error {
        // Increment the counter atomically
        newID := atomic.AddUint64(&counter, 1)
        body := map[string]any{
            "type": "generate_ok",
            "id":   fmt.Sprintf("%s%d", n.ID(), newID),
        }

        n.Reply(msg, body)
        return nil
    })
}
