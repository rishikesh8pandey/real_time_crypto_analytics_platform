from kafka import KafkaProducer
import json
import websocket
import time
import logging


BOOTSTRAP_SERVERS = "localhost:29092"
Topic_name = 'crypto_ticks'



producer = KafkaProducer(
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    linger_ms=10,
                    batch_size=16384,
                    request_timeout_ms=30000
                )



def on_send_success(record_metadata):
    logging.info(
        f"Message sent | topic={record_metadata.topic}, "
        f"partition={record_metadata.partition}, offset={record_metadata.offset}"
    )



def on_send_error(error):
    logging.error(f"Kafka send failed: {error}")


def send_to_kafka(Topic_name, key, value):
    if value["symbol"] is None:  
        logging.info("Symbol is Null")
        return
        
    if float(value["price"]) <= 0:
        logging.info("Price is invalid")
        return

    if value["event_time"] is None:
        logging.info("Event time is Null")
        return
    
    if value["trade_id"] is None:
        logging.info("trade_id is Null")
        return
    
    if float(value["quantity"]) <= 0:
        logging.info("Quantity is invalid")
        return

    print(Topic_name,key,value) 
    producer.send(
    Topic_name,
    key=key,
    value=value).add_callback(on_send_success).add_errback(on_send_error)



def on_message(ws, message):
    response = json.loads(message)

    i = response["data"]

    crypto_data = {
            "trade_id": i["t"],
            "symbol": i["s"],
            "price": float(i["p"]),
            "quantity": float(i["q"]),
            "volume": float(i["p"]) * float(i["q"]),
            "event_time": i["T"],
            "source": "binance"
}

    send_to_kafka(Topic_name,crypto_data["symbol"],crypto_data)



def on_error(ws, error):
    logging.error(f"Encountered error: {error}")
    print(f"Encountered error: {error}")



def on_close(ws, close_status_code, close_msg):
    logging.warning("Disconnected")
    print("Closed")

    

def on_open(ws):
    logging.info("Connected to Binance")
    logging.info("Program Started")
    print("Connected")
    


if __name__ == "__main__":
    logging.basicConfig(
    filename="producer.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s")

    while True:
        ws = websocket.WebSocketApp('wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/solusdt@trade',
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
        ws.run_forever()

        print("Attempting reconnect...")
        time.sleep(5)