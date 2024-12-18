# -*- coding: utf-8 -*-
from awscrt import mqtt
from awsiot import mqtt_connection_builder
import json
import time
from datetime import datetime
from gpiozero import LED
import threading  # threading 모듈 임포트

# AWS IoT Core 설정
ENDPOINT = ""
CLIENT_ID = ""
TOPIC_SUBSCRIBE = ""

# 인증서 경로
CA_PATH = ""
CERT_PATH = ""
KEY_PATH = ""

# LED 핀 설정
LED_PIN = 17
led = LED(LED_PIN)

# 가장 최신의 타임스탬프를 저장할 변수 초기화
latest_timestamp = None

# MQTT 연결 설정
mqtt_connection = mqtt_connection_builder.mtls_from_path(
    endpoint=ENDPOINT,
    cert_filepath=CERT_PATH,
    pri_key_filepath=KEY_PATH,
    ca_filepath=CA_PATH,
    client_id=CLIENT_ID,
    clean_session=True,
    keep_alive_secs=30
)

print(f"Connecting to {ENDPOINT} with client ID '{CLIENT_ID}'...")
connect_future = mqtt_connection.connect()
connect_future.result()
print("Connected!")

# LED 깜빡임을 처리하는 함수 (별도의 스레드에서 실행)
def blink_led():
    for _ in range(4):
        led.on()
        time.sleep(0.25)
        led.off()
        time.sleep(0.25)

# MQTT 수신 콜백 함수
def on_message_received(topic, payload, **kwargs):
    global latest_timestamp
    try:
        # JSON 데이터를 파싱
        data = json.loads(payload)
        strtime = data.get('timestamp')
        if not strtime:
            print("No timestamp in message.")
            return

        message_time = datetime.strptime(strtime, "%Y-%m-%d %H:%M:%S")

        if "mq_value" in data:
            mq_value = data["mq_value"]
            print(f"data: {payload}")

            # 특정 임계값 이상이면 LED 깜빡이기
            if mq_value > 800:
                print("MQ value exceeds threshold, blinking LED.")
                # LED 깜빡임을 별도의 스레드에서 실행
                threading.Thread(target=blink_led).start()
            else:
                print("MQ value below threshold, turning LED off.")
                led.off()
        else:
            pass
    except json.JSONDecodeError:
        print("Invalid payload format.")
    except ValueError as e:
        print(f"Timestamp parsing error: {e}")

# MQTT 구독 설정
subscribe_future, packet_id = mqtt_connection.subscribe(
    topic=TOPIC_SUBSCRIBE,
    qos=mqtt.QoS.AT_MOST_ONCE,
    callback=on_message_received
)
subscribe_result = subscribe_future.result()
print(f"Subscribed with QoS {subscribe_result['qos']}")

try:
    print(f"Listening for messages on topic '{TOPIC_SUBSCRIBE}'...")
    while True:
        time.sleep(0.5)
except KeyboardInterrupt:
    print("Interrupted, disconnecting...")
finally:
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    led.off()
    print("Disconnected!")
