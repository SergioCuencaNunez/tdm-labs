import json
import random
import re
import string
import time
from datetime import datetime
from websocket import create_connection, WebSocketConnectionClosedException
from kafka import KafkaProducer

# Configuración de Kafka
KAFKA_BROKER = '192.168.80.34:9092'
KAFKA_TOPIC = 'gittba_SOL'

# Crea el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_session():
    string_length = 12
    letters = string.ascii_lowercase
    return "qs_" + "".join(random.choice(letters) for _ in range(string_length))

def prepend_header(content):
    return f"~m~{len(content)}~m~{content}"

def construct_message(func, param_list):
    return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))

def create_message(func, param_list):
    return prepend_header(construct_message(func, param_list))

def send_message(ws, func, args):
    try:
        ws.send(create_message(func, args))
    except WebSocketConnectionClosedException:
        print("Conexion cerrada mientras se intentaba enviar un mensaje.")
        reconnect(ws)

def send_ping(ws):
    try:
        ping_message = "~h~0"
        ws.send(ping_message)
    except Exception as e:
        print(f"Error enviando ping: {e}")

def process_data(data):
    price = data.get("lp", "No disponible")
    change = data.get("ch", "No disponible")
    change_percentage = data.get("chp", "No disponible")
    volume = data.get("volume", "No disponible")
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

    payload = {
        "timestamp": timestamp,
        "price": price,
        "change": change,
        "change_percentage": change_percentage,
        "volume": volume
    }

    print(f"[{timestamp}] Enviando a Kafka: {payload}")
    producer.send(KAFKA_TOPIC, key="solana", value=payload)
    producer.flush()

def reconnect(symbol_id):
    print("Intentando reconectar...")
    time.sleep(5)
    start_socket(symbol_id)

def start_socket(symbol_id):
    session = generate_session()
    url = "wss://data.tradingview.com/socket.io/websocket"
    headers = json.dumps({"Origin": "https://data.tradingview.com"})

    try:
        ws = create_connection(url, headers=headers)
        print(f"Conectado a {url}")

        send_message(ws, "quote_create_session", [session])
        send_message(ws, "quote_set_fields", [session, "lp", "ch", "chp", "volume"])
        send_message(ws, "quote_add_symbols", [session, symbol_id])

        while True:
            try:
                result = ws.recv()
                if result.startswith("~m~"):
                    data_match = re.search(r"\{.*\}", result)
                    if data_match:
                        message = json.loads(data_match.group(0))
                        if message["m"] == "qsd":
                            process_data(message["p"][1]["v"])
                elif result.startswith("~h~"):
                    send_ping(ws)

            except WebSocketConnectionClosedException:
                print("Conexión cerrada inesperadamente.")
                reconnect(symbol_id)
                break
            except Exception as e:
                print(f"Error procesando mensaje: {e}")
                continue

    except WebSocketConnectionClosedException as e:
        print(f"Error al conectar: {e}. Reconectando en 5 segundos...")
        reconnect(symbol_id)
    except Exception as e:
        print(f"Error inesperado: {e}. Reconectando en 5 segundos...")
        reconnect(symbol_id)

if __name__ == "__main__":
    symbol_id = "BINANCE:SOLUSDT"
    start_socket(symbol_id)