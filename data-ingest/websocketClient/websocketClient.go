package websocketClient

import (
	"fmt"
	"log"
	"net/url"

	"github.com/gorilla/websocket"
)

// websocketClient struct definition
type websocketClient struct {
	conn *websocket.Conn
}

// NewWebsocketClient is a constructor for websocketClient
func NewWebsocketClient() *websocketClient {
	return &websocketClient{} // Empty connection object initialised
}

func (c *websocketClient) NewWebsocketConnection(binanceInfo BinanceInfo) error {
	// Connect to Binance WebSocket
	binanceURL := url.URL{
		Scheme: binanceInfo.BinanceScheme,
		Host:   binanceInfo.BinanceHost,
		Path:   binanceInfo.BinancePath,
	}
	log.Printf("Connecting to Binance WebSocket at %s\n", binanceURL.String())
	websocketConn, _, err := websocket.DefaultDialer.Dial(binanceURL.String(), nil)
	c.conn = websocketConn

	if err != nil {
		// NOTE: log.Fatalf will terminate the program, so we return nil and err instead
		log.Fatalf("[NewWebsocketConnection] Failed to connect to Binance WebSocket due to error: %s", err.Error())
		return nil
	}

	return nil
}

func (c *websocketClient) CloseConnection() {
	if c.conn == nil {
		log.Println("[CloseConnection] No WebSocket connection to close.")
		return
	}

	if err := c.conn.Close(); err != nil {
		log.Printf("[CloseConnection] Failed to close WebSocket connection: %s", err.Error())
	} else {
		log.Println("[CloseConnection] WebSocket connection closed successfully.")
	}
}

func (c *websocketClient) SubscribeToStreams(streamNames []string) error {
	if c.conn == nil {
		log.Println("[SubscribeToStream] No WebSocket connection established.")
		return fmt.Errorf("[SubscribeToStream] No WebSocket connection established")
	}

	subscribe := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": streamNames,
		"id":     1,
	}
	if err := c.conn.WriteJSON(subscribe); err != nil {
		log.Fatalf("[SubscribeToStream] Failed to subscribe to streams, err: %s", err.Error())
		return err
	}
	log.Printf("[SubscribeToStream] Successfully subscribed to streams: %v", streamNames)

	return nil
}

func (c *websocketClient) UnsubscribeFromStreams(streamNames []string) error {
	if c.conn == nil {
		log.Println("[UnsubscribeFromStream] No WebSocket connection established.")
		return fmt.Errorf("[UnsubscribeFromStream] No WebSocket connection established")
	}

	subscribe := map[string]interface{}{
		"method": "UNSUBSCRIBE",
		"params": streamNames,
		"id":     1,
	}
	if err := c.conn.WriteJSON(subscribe); err != nil {
		log.Fatalf("[UnsubscribeFromStream] Failed to unsubscribe from streams, err: %s", err.Error())
		return err
	}
	log.Printf("[UnsubscribeFromStream] Successfully unsubscribed from streams: %v", streamNames)

	return nil
}

func (c *websocketClient) ReadMessages(tradeInfoChannel chan<- string) { // NOTE: chan<- string defines a send-only channel
	if c.conn == nil {
		log.Println("[ReadMessages] No WebSocket connection to read from.")
		return
	}

	// Infinite loop until "return"/"break" is called
	for {
		// Message type, message, error
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			log.Printf("[ReadMessages] Error reading message: %s", err.Error())
			return
		}

		select {
		case tradeInfoChannel <- string(message): // Send message to channel
			log.Printf("[ReadMessages] Received message, passed to channel: %s", string(message))
		default: // Message dropped if bounded channel is full
			log.Println("[ReadMessages] Warning: broadcast channel is full, dropping message")
		}
	}
}
