# import paho.mqtt.client as mqtt
# import json
# import time
# from influxdb import InfluxDBClient
# from collections import defaultdict
# from datetime import datetime, timedelta

# # Configurações do MQTT Broker
# MQTT_BROKER = "localhost"
# MQTT_PORT = 1883
# MQTT_KEEPALIVE_INTERVAL = 60

# # Configurações do InfluxDB
# INFLUXDB_HOST = "localhost"
# INFLUXDB_PORT = 8086
# INFLUXDB_DATABASE = "sensor_data"

# # Conectar ao InfluxDB
# db_client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT)
# db_client.switch_database(INFLUXDB_DATABASE)

# # Armazenar última leitura de cada sensor
# last_reading = defaultdict(lambda: datetime.utcnow())


# # Função para persistir dados no InfluxDB
# def persist_data(machine_id, sensor_id, value):
#     json_body = [
#         {
#             "measurement": f"{machine_id}.{sensor_id}",
#             "tags": {"machine_id": machine_id, "sensor_id": sensor_id},
#             "time": datetime.utcnow().isoformat(),
#             "fields": {"value": value},
#         }
#     ]
#     db_client.write_points(json_body)


# # Função para gerar alarmes de inatividade
# def check_inactivity():
#     while True:
#         now = datetime.utcnow()
#         for (machine_id, sensor_id), last_time in last_reading.items():
#             if now - last_time > timedelta(
#                 seconds=50
#             ):  # 10 vezes o intervalo mais curto (5 segundos)
#                 persist_data(machine_id, "alarms.inactive", 1)
#         time.sleep(10)


# # Função de callback quando uma mensagem é recebida
# def on_message(client, userdata, message):
#     data = json.loads(message.payload)
#     topic_parts = message.topic.split("/")
#     machine_id = topic_parts[2]
#     sensor_id = topic_parts[3]

#     if sensor_id == "inactive":
#         return  # Ignorar mensagens de alarme

#     last_reading[(machine_id, sensor_id)] = datetime.utcnow()
#     persist_data(machine_id, sensor_id, data["value"])


# # Função de callback quando um novo sensor é detectado
# def on_new_sensor(client, userdata, message):
#     data = json.loads(message.payload)
#     machine_id = data["machine_id"]
#     for sensor in data["sensors"]:
#         sensor_id = sensor["sensor_id"]
#         client.subscribe(f"/sensors/{machine_id}/{sensor_id}")


# # Configurar o cliente MQTT
# client = mqtt.Client()
# client.on_message = on_message

# # Inscrever-se no tópico /sensor_monitors para detectar novos sensores
# client.message_callback_add("/sensor_monitors", on_new_sensor)
# client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE_INTERVAL)
# client.subscribe("/sensor_monitors")

# # Iniciar thread para verificar inatividade
# inactivity_thread = threading.Thread(target=check_inactivity)
# inactivity_thread.start()

# # Iniciar loop do cliente MQTT
# client.loop_forever()


import paho.mqtt.client as mqtt
import json
from influxdb import InfluxDBClient

# Configurações do MQTT
broker = "localhost"
port = 1883
topic = "/sensors/machine/temperature"

# Configurações do InfluxDB
influxdb_client = InfluxDBClient(host='localhost', port=8086)
influxdb_client.switch_database('sensors_data')

# Função de conexão do MQTT
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe(topic)

# Função para processar mensagens do MQTT
def on_message(client, userdata, msg):
    data = json.loads(msg.payload)
    json_body = [
        {
            "measurement": "temperature",
            "tags": {
                "sensor": "temperature_sensor"
            },
            "time": data["timestamp"],
            "fields": {
                "value": data["value"]
            }
        }
    ]
    influxdb_client.write_points(json_body)

# Configuração do cliente MQTT
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(broker, port, 60)
client.loop_forever()
