package handlers

import (
	"encoding/json"
	"fmt"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func RegisterSingleNodeBroadcastHandler(n *maelstrom.Node) {
	messages := []int{}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "broadcast_ok"
		if msg_val, ok := body["message"].(float64); !ok {
			// Handle the case where the assertion fails
			return fmt.Errorf("message is not of type int %v", body["message"])
		} else {
			delete(body, "message")
			msg_val_int := int(msg_val)
			messages = append(messages,msg_val_int)
		}
		n.Reply(msg, body)
		return nil
		
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "read_ok"
		body["messages"] = messages
		n.Reply(msg, body)
		return nil
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		delete(body, "topology")
		body["type"] = "topology_ok"
		n.Reply(msg, body)
		return nil
	})

}