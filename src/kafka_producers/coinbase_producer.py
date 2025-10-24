from kafka import KafkaProducer
import json, websocket, threading

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def on_message(ws, message):
    producer.send('coinbase_trades', message.encode())
    print("Sent Coinbase message (truncated):", message[:100])

def on_error(ws, error):
    print("WebSocket error:", error)

def on_close(ws):
    print("WebSocket closed")

def on_open(ws):
    subscribe_message = {
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channels": ["matches"]
    }
    ws.send(json.dumps(subscribe_message))

def run_ws():
    ws = websocket.WebSocketApp(
        "wss://ws-feed.exchange.coinbase.com",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()

if __name__ == "__main__":
    t = threading.Thread(target=run_ws)
    t.start()
    t.join()
