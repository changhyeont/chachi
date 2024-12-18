# -*- coding: utf-8 -*-
import serial
import ssl
import json
import time
import datetime
import warnings
import time
import spidev
import paho.mqtt.client as mqtt

warnings.filterwarnings("ignore", category=DeprecationWarning)
# AWS IoT Core configuration
ENDPOINT = ""
CLIENT_ID = "i"
TOPIC = ""
CA_PATH = ""
CERT_PATH = ""
KEY_PATH = "/"


SERIAL_PORT = "/dev/ttyAMA0"  #  ?   ?    ?             "/dev/serial0"
BAUD_RATE = 9600

# MQTT client setup
client = mqtt.Client(clean_session=True)
client.client_id = CLIENT_ID
client.tls_set(ca_certs=CA_PATH, certfile=CERT_PATH, keyfile=KEY_PATH, tls_version=ssl.PROTOCOL_TLSv1_2)

def connect_mqtt():
    try:
        client.connect(ENDPOINT, 8883)
        print("Connected to AWS IoT Core successfully.")
    except Exception as e:
        print(f"Failed to connect to AWS IoT Core: {e}")
        #time.sleep(5)
        connect_mqtt()

def read_gps_data():
    gps_data = {}
    try:
        with serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1) as ser:
            while True:
                line = ser.readline().decode('utf-8', errors='replace').strip()
                if "$GPRMC" in line:
                    parts = line.split(',')
                    if len(parts) >= 12 and parts[2] == 'A':  # ��ȿ�� ������ Ȯ��
                        # GPS ��ǥ ��ȯ
                        lat = float(parts[3][:2]) + float(parts[3][2:]) / 60
                        lon = float(parts[5][:3]) + float(parts[5][3:]) / 60
                        
                        if parts[4] == 'S': lat = -lat
                        if parts[6] == 'W': lon = -lon
                        
                        gps_data['latitude'] = round(lat, 6)
                        gps_data['longitude'] = round(lon, 6)
                        
                        return gps_data
    except serial.SerialException as e:
        print(f"port error: {e}")
        #time.sleep(2)
    except Exception as e:
        print(f"date error: {e}")
    return gps_data

# SPI setup for MQ sensor
spi = spidev.SpiDev()
spi.open(0, 0)
spi.max_speed_hz = 1350000

# Function to read data from MQ sensor
def read_mq_sensor(channel=0):
    adc = spi.xfer2([1, (8 + channel) << 4, 0])
    data = ((adc[1] & 3) << 8) + adc[2]
    return float(data)

# Start MQTT connection
client.loop_start()
connect_mqtt()

try:
    while True:
        # Get the current timestamp
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Read MQ sensor data
        mq_value = read_mq_sensor()
        latitude = read_gps_data().get('latitude')
        longitude = read_gps_data().get('longitude')
        # Create data payload
        all_data = {
            "timestamp": timestamp,
            "latitude": latitude,
            "longitude": longitude,
            "mq_value": mq_value
        }
        # Publish data to MQTT topic
        payload = json.dumps(all_data)
        try:
            client.publish(TOPIC, payload, qos=0, retain=False)
            print(f"data: {payload}")
        except Exception as e:
            print(f"data fail: {e}")
            connect_mqtt()  # Retry connection

        # Wait before sending the next data
        time.sleep(1)

except KeyboardInterrupt:
    print("Terminating the program.")

finally:
    # Disconnect MQTT and close SPI
    client.loop_stop()
    client.disconnect()
    spi.close()
    print("Disconnected!")
