package handlers

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type NetworkMessages struct {
	messages []int
	messageMap map[string]int
}

func (network_messages *NetworkMessages) AddMessage(msgID string, msgVal int) {
	if _, exists := network_messages.messageMap[msgID]; !exists {
		network_messages.messages = append(network_messages.messages, msgVal)
		network_messages.messageMap[msgID] = msgVal
	} else {
		fmt.Fprintf(os.Stderr, "Message %v already exists in the network\n", msgID)
	}
}

func (network_messages *NetworkMessages) ReadMessage() []int {
	return network_messages.messages
}

func handleClientMessage(n *maelstrom.Node ,network_messages *NetworkMessages, body map[string]any, allNodes []string, currentNodeID string) {
	// Create a new body from the existing body and add the parent field
	newBody := make(map[string]any)
	for k, v := range body {
		newBody[k] = v
	}
	newBody["is_client_msg"] = false
	
	if msgVal, ok := body["message"].(float64); ok {
		// Type assertion for "msg_id"
		if msgID, ok := body["msg_id"].(float64); ok {
			network_messages.AddMessage(currentNodeID + strconv.Itoa(int(msgID)), int(msgVal))
		} else {
			fmt.Errorf("msg_id is not a valid number: %v", body["msg_id"])
		}
	} else {
			fmt.Errorf("message is not a valid number: %v", body["message"])
	}
	
	


	for _, node := range allNodes {
		if node != currentNodeID {
			fmt.Fprintf(os.Stderr, "sending message to node %v from node %v\n", node, currentNodeID)
			if err := n.Send(node, newBody); err != nil {
				fmt.Fprintf(os.Stderr, "Error sending message to node %v: %v\n", node, err)
			}
		}
	}
}

func handleBroadcastMessage(source_node string, currentNodeID string, body map[string]any, network_messages *NetworkMessages) {
	if msgVal, ok := body["message"].(float64); ok {
		// Type assertion for "msg_id"
		fmt.Fprintf(os.Stderr, "source node is %v and msg is %v, current node is %v\n",source_node, msgVal,currentNodeID)
		if msgID, ok := body["msg_id"].(float64); ok {
			network_messages.AddMessage(source_node + strconv.Itoa(int(msgID)), int(msgVal))
		} else {
			fmt.Errorf("msg_id is not a valid string: %v", body["msg_id"])
		}
	} else {
			fmt.Errorf("message is not a valid number: %v", body["message"])
	}
	fmt.Fprintf(os.Stderr, "The current message on node %v is not from client but broadcasted\n", currentNodeID)
}

func RegisterMultiNodeBroadcastHandler(n *maelstrom.Node) {
	network_messages := NetworkMessages{messages: make([]int, 0), messageMap: make(map[string]int)}
	adjacencyList := make(map[string][]string)
	
	allNodes := make([]string, 0)
	var currentNodeID string

	n.Handle("init", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		currentNodeID = body["node_id"].(string)
		for _, node_id := range body["node_ids"].([]interface{}) {
			allNodes = append(allNodes, node_id.(string))
		}
		fmt.Fprintf(os.Stderr, "All nodes: %v %v\n", allNodes, currentNodeID)
		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		fmt.Fprintf(os.Stderr, "the network message %v\n", network_messages)	
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}



		if isClientMsg, ok := body["is_client_msg"].(bool); ok && !isClientMsg {
			handleBroadcastMessage(msg.Src, currentNodeID, body, &network_messages)
		} else {
			handleClientMessage(n, &network_messages, body, allNodes, currentNodeID)
		}
			

			// Iterate over the neighboring nodes
			// if neighbors, exists := adjacencyList[currentNodeID]; exists {
			// 	for _, neighbor := range neighbors {
			// 		// Check if the neighbor is the parent
			// 		if parent, ok := body["parent"].(string); ok && neighbor == parent {
			// 			continue // Skip broadcasting to the parent node
			// 		}
			// 		fmt.Printf("Sending message to neightbor %v from node %v\n",neighbor, currentNodeID)
			// 		// Send the message to the neighboring node
			// 		if err := n.Send(neighbor, newBody); err != nil {
			// 			return err
			// 		}
			// 	}
			// } else {
			// 	fmt.Fprintf(os.Stderr, "Node %v has no neighbors\n", currentNodeID)
			// }

		


		delete(body, "message")
		body["type"] = "broadcast_ok"

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = network_messages.ReadMessage()

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
	
		// Log the entire body to check its contents
		fmt.Fprintf(os.Stderr, "Received body: %v\n", body)

		if topology, ok := body["topology"].(map[string]any); ok {
			for node, neighbors := range topology {
				if neighborsList, ok := neighbors.([]interface{}); ok {
					adjacencyList[node] = make([]string, len(neighborsList))
					for i, neighbor := range neighborsList {
						adjacencyList[node][i] = neighbor.(string)
					}
				} else {
					fmt.Fprintf(os.Stderr, "Error: neighbors for node %v are not in the expected format\n", node)
				}
			}
		} else {
			fmt.Fprintf(os.Stderr, "Error: topology field is missing or not in the expected format\n")
		}
	
		delete(body, "topology")
		body["type"] = "topology_ok"
		fmt.Fprintf(os.Stderr, "network topology: %v\n", adjacencyList)
	
		return n.Reply(msg, body)
	})
}
