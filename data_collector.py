# import paho.mqtt.client as mqtt
# import json
# import uuid
# import time
# import threading
# import random
# from datetime import datetime

# # Configurações do MQTT Broker
# MQTT_BROKER = "localhost"
# MQTT_PORT = 1883
# MQTT_KEEPALIVE_INTERVAL = 60

# # Identificador único da máquina
# machine_id = str(uuid.uuid4())

# # Sensores simulados
# sensors = [
#     {"id": "temperature", "type": "float", "interval": 5},
#     {"id": "humidity", "type": "float", "interval": 7},
# ]


# # Função para publicar dados do sensor
# def publish_sensor_data(sensor):
#     client = mqtt.Client()
#     client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE_INTERVAL)

#     while True:
#         timestamp = datetime.utcnow().isoformat() + "Z"
#         value = (
#             random.uniform(20.0, 30.0)
#             if sensor["id"] == "temperature"
#             else random.uniform(40.0, 60.0)
#         )
#         message = json.dumps({"timestamp": timestamp, "value": value})
#         topic = f"/sensors/{machine_id}/{sensor['id']}"
#         client.publish(topic, message)
#         time.sleep(sensor["interval"])


# # Função para publicar mensagem inicial
# def publish_initial_message():
#     client = mqtt.Client()
#     client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE_INTERVAL)

#     while True:
#         sensors_info = [
#             {
#                 "sensor_id": sensor["id"],
#                 "data_type": sensor["type"],
#                 "data_interval": sensor["interval"],
#             }
#             for sensor in sensors
#         ]
#         message = json.dumps({"machine_id": machine_id, "sensors": sensors_info})
#         client.publish("/sensor_monitors", message)
#         time.sleep(60)  # Enviar a cada 60 segundos


# # Criar threads para cada sensor
# threads = [
#     threading.Thread(target=publish_sensor_data, args=(sensor,)) for sensor in sensors
# ]
# threads.append(threading.Thread(target=publish_initial_message))

# # Iniciar threads
# for thread in threads:
#     thread.start()

import paho.mqtt.client as mqtt
import time
import json
import random

# Configurações do MQTT
broker = "localhost"
port = 1883
topic = "/sensors/machine/temperature"

# Função de conexão do MQTT
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe(topic)

# Configuração do cliente MQTT
client = mqtt.Client()
client.on_connect = on_connect
client.connect(broker, port, 60)

# Publicar dados dos sensores
def publish_sensor_data():
    while True:
        temperature = random.uniform(20.0, 30.0)
        payload = {
            "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            "value": temperature
        }
        client.publish(topic, json.dumps(payload))
        time.sleep(5)

publish_sensor_data()
