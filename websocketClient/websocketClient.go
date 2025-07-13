package websocketClient

import (
	"log"
	"net/url"

	"github.com/gorilla/websocket"
)

// websocketClient struct definition
type websocketClient struct {
	// Add fields if needed, e.g. API keys, config, etc.
}

// NewWebsocketClient is a constructor for websocketClient
func NewWebsocketClient() *websocketClient {
	return &websocketClient{}
}

func (c *websocketClient) NewWebsocketConnection(binanceInfo BinanceInfo) (*websocket.Conn, error) {
	// Connect to Binance WebSocket
	binanceURL := url.URL{
		Scheme: binanceInfo.BinanceScheme,
		Host:   binanceInfo.BinanceHost,
		Path:   binanceInfo.BinancePath,
	}
	log.Printf("Connecting to Binance WebSocket at %s\n", binanceURL.String())
	websocketConn, _, err := websocket.DefaultDialer.Dial(binanceURL.String(), nil)
	if err != nil {
		log.Fatalf("[NewWebsocketConnection] Failed to connect to Binance WebSocket due to error: %s", err.Error())
		return nil, err
	}

	return websocketConn, nil
}

func (c *websocketClient) CloseConnection(conn *websocket.Conn) {
	if err := conn.Close(); err != nil {
		log.Printf("[CloseConnection] Failed to close WebSocket connection: %s", err.Error())
	} else {
		log.Println("[CloseConnection] WebSocket connection closed successfully.")
	}
}

func (c *websocketClient) SubscribeToStream(conn *websocket.Conn, streamNames []string) error {
	subscribe := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": streamNames,
		"id":     1,
	}
	if err := conn.WriteJSON(subscribe); err != nil {
		log.Fatalf("[SubscribeToStream] Failed to subscribe to streams, err: %s", err.Error())
		return err
	}
	log.Printf("[SubscribeToStream] Successfully subscribed to streams: %v", streamNames)

	return nil
}

func (c *websocketClient) ReadMessages(conn *websocket.Conn, tradeInfoChannel chan<- string) {
	for {
		// Message type, message, error
		_, message, err := conn.ReadMessage()
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
