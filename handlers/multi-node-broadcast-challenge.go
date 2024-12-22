package handlers

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type NetworkMessages struct {
	mu          sync.Mutex
	messages []int
	messageMap map[string]int
}

type BroadcastMethod int

const (
    DIRECT_BROADCAST_METHOD BroadcastMethod = iota
    RING_BROADCAST_METHOD
)


func (network_messages *NetworkMessages) AddMessage(msgID string, msgVal int) {
	network_messages.mu.Lock()
	
	if _, exists := network_messages.messageMap[msgID]; !exists {
		network_messages.messages = append(network_messages.messages, msgVal)
		network_messages.messageMap[msgID] = msgVal
	}
    network_messages.mu.Unlock()
}

func (network_messages *NetworkMessages) ReadMessage() []int {
	network_messages.mu.Lock()
    defer network_messages.mu.Unlock()
	return network_messages.messages
}

func (network_messages *NetworkMessages) ReadMessageMap() map[string]int {
	network_messages.mu.Lock()
    defer network_messages.mu.Unlock()// Create a copy of the map to safely return
	copyMap := make(map[string]int, len(network_messages.messageMap))
	for k, v := range network_messages.messageMap {
		copyMap[k] = v
	}
	return copyMap
}

func handleClientMessage(n *maelstrom.Node ,network_messages *NetworkMessages, body map[string]any, allNodes []string, currentNodeID string, broadcast_method BroadcastMethod) {
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
	
	
	switch broadcast_method {
		case DIRECT_BROADCAST_METHOD:
			// direct broadcast to all nodes
			for _, node := range allNodes {
				if node != currentNodeID {
					fmt.Fprintf(os.Stderr, "sending message to node %v from node %v\n", node, currentNodeID)
					if err := n.Send(node, newBody); err != nil {
						fmt.Fprintf(os.Stderr, "Error sending message to node %v: %v\n", node, err)
					}
				}
			}
		case RING_BROADCAST_METHOD:
			fmt.Fprintf(os.Stderr, "Ring broadcast method is executed in separate coroutine\n")
	}
}

func startNodeSyncing(n *maelstrom.Node, network_messages *NetworkMessages, allNodes []string, currentNodeID string) {
	//sync messages between nodes in ring topology

	getNextNode := func(allNodes []string, currentNodeID string) string {
		for i, nodeID := range allNodes {
			if nodeID == currentNodeID {
				return allNodes[(i+1)%len(allNodes)]
			}
		}
		return "" // Return empty string if node not found (shouldn't happen)
	}

	go func() {
		for {
			// Determine the next node in the ring (circular list)
			nextNode := getNextNode(allNodes, currentNodeID)


			fmt.Fprintf(os.Stderr, "syncing node %v with node %v\n", currentNodeID, nextNode)
				
			// Create the message body containing the current node's messages to be sent
			messageBody := map[string]interface{}{
				"type":            "sync_request", // Custom message type
				"message_map":        network_messages.ReadMessageMap(), // Current node's message buffer
				"current_node_id": currentNodeID, // Include current node ID to identify the sender
			}
			
			// Send the RPC request to the next node
			err := n.RPC(nextNode, messageBody, func(resp maelstrom.Message) error {
				// Handle the response received from the next node (sync response)
				var responseBody map[string]interface{}
				if err := json.Unmarshal(resp.Body, &responseBody); err != nil {
					return err
				}
				
				fmt.Fprintf(os.Stderr, "response body of node sync\n", responseBody)

				if message_map, ok := responseBody["message_map"].(map[string]interface{}); ok {
					for msgID, msgVal := range message_map {
						if msgValFloat, ok := msgVal.(float64); ok {
							network_messages.AddMessage(msgID, int(msgValFloat))
						} else {
							return fmt.Errorf("message value is not a valid float64: %v", msgVal)
						}
					}
				}
				return nil
			})

			if err != nil {
				fmt.Fprintf(os.Stderr, "Error sending sync request to node %v: from node %v error %v\n", nextNode, currentNodeID, err)
			}
			// Sleep for 500 milliseconds before syncing again
			time.Sleep(1000 * time.Millisecond)
		}
	}()
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
	var broadcast_method BroadcastMethod
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
		broadcast_method = RING_BROADCAST_METHOD	
		if broadcast_method == RING_BROADCAST_METHOD {
			startNodeSyncing(n, &network_messages, allNodes, currentNodeID)
		}
		return nil
	})

	n.Handle("sync_request", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "sync_response"

		fmt.Fprintf(os.Stderr, "Received sync request from %v to %v\n", msg.Src, currentNodeID)

		if message_map, ok := body["message_map"].(map[string]interface{}); ok {
			for msgID, msgVal := range message_map {
				if msgValFloat, ok := msgVal.(float64); ok {
					network_messages.AddMessage(msgID, int(msgValFloat))
				} else {
					return fmt.Errorf("message value is not a valid float64: %v", msgVal)
				}
			}
		}

		body["message_map"] = network_messages.ReadMessageMap()

		return n.Reply(msg, body)

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
			handleClientMessage(n, &network_messages, body, allNodes, currentNodeID, broadcast_method)
		}

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
