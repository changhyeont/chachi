# -*- coding: utf-8 -*-
from awscrt import mqtt
from awsiot import mqtt_connection_builder
import json
import time
from datetime import datetime
from gpiozero import LED
import threading  # threading 모듈 임포트
import requests

def check_nearby_bars(lat, lon, radius=1000, api_key=""):
    """주변 술집 검색 함수"""
    url = ""
    headers = {
        "Authorization": f"KakaoAK {api_key}"
    }
    params = {
        "query": "술집",
        "x": lon,
        "y": lat,
        "radius": radius,
        "size": 10
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # HTTP 오류 체크
        
        data = response.json()
        if data['documents']:
            print("\n주차된 장소 주변 주점 정보:")
            for bar in data['documents']:
                name = bar['place_name']
                address = bar['address_name']
                distance = bar['distance']
                print(f"- {name} | 주소: {address} | 거리: {distance}m")
            print(f"주점 개수: {len(data['documents'])}")
            print("trigger")
        else:
            print("\n주차된 장소 주변에 주점이 존재하지 않습니다.")
            
    except Exception as e:
        print(f"\n주변 술집 검색 중 오류 발생: {str(e)}")

class Timer:
    def __init__(self, duration_minutes=5):
        self.duration = duration_minutes * 60
        self.is_running = False
        self.thread = None
        self.last_latitude = None
        self.last_longitude = None
    
    def start(self):
        if not self.is_running:
            self.is_running = True
            self.thread = threading.Thread(target=self._run_timer)
            self.thread.start()
    
    def stop(self):
        self.is_running = False
        if self.thread:
            self.thread.join()
    
    def reset(self):
        self.stop()
        self.start()
    
    def _run_timer(self):
        start_time = time.time()
        while self.is_running:
            elapsed = int(time.time() - start_time)
            remaining = self.duration - elapsed
            
            if remaining <= 0:
                print("\n5분이 경과했습니다! 자동차를 주차한 것으로 판단됩니다.")
                if self.last_latitude and self.last_longitude:
                    check_nearby_bars(self.last_latitude, self.last_longitude)
                self.is_running = False
                break
            
            minutes = remaining // 60
            seconds = remaining % 60
            print(f"남은 시간: {minutes:02d}:{seconds:02d}", end='\r')
            time.sleep(1)

# PRED 변수 초기화
PRED = False

# MQTT 수신 콜백 함수 수정
def on_message_received(topic, payload, **kwargs):
    global last_message_time, PRED
    try:
        data = json.loads(payload)
        print(f"수신된 데이터: {data}")
        
        # 위치 정보 저장
        if "latitude" in data and "longitude" in data:
            timer.last_latitude = data["latitude"]
            timer.last_longitude = data["longitude"]
        
        # prediction 키가 있고, 그 값이 "비정상"인 경우에만 PRED 업데이트
        if "prediction" in data:
            if data["prediction"] == "비정상":
                print("비정상 경로 감지! LED 깜빡임 시작...")
                threading.Thread(target=blink_led).start()
                PRED = True
            else:
                PRED = False
        
        # 메시지 수신 시간 업데이트
        last_message_time = time.time()
        timer.stop()  # 메시지 수신 시 타이머 중지
            
    except json.JSONDecodeError:
        print("잘못된 JSON 형식입니다.")
    except Exception as e:
        print(f"오류 발생: {str(e)}")

# 타이머 체크 스레드
def check_message_timeout():
    global last_message_time, PRED
    while True:
        current_time = time.time()
        # PRED가 비정상이고, 20초 동안 메시지 없음
        if PRED and current_time - last_message_time >= 20 and not timer.is_running:
            print("\n메시지 수신 없음. 타이머 시작...")
            timer.start()
        time.sleep(1)

# AWS IoT Core 설정
ENDPOINT = ""
CLIENT_ID = ""
TOPIC_SUBSCRIBE = ""

# 인증서 경로
CA_PATH = ""
CERT_PATH = ""
KEY_PATH = ""

# LED 핀 설정
LED_PIN = 16
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

# 타이머 인스턴스 생성
timer = Timer(5)
last_message_time = time.time()

# MQTT 구독 설정
subscribe_future, packet_id = mqtt_connection.subscribe(
    topic=TOPIC_SUBSCRIBE,
    qos=mqtt.QoS.AT_MOST_ONCE,
    callback=on_message_received
)
subscribe_result = subscribe_future.result()
print(f"Subscribed with QoS {subscribe_result['qos']}")

# 메인 코드에 타이머 체크 스레드 시작 추가
if __name__ == "__main__":
    # 타이머 체크 스레드 시작
    timer_check_thread = threading.Thread(target=check_message_timeout)
    timer_check_thread.daemon = True  # 메인 프로그램 종료시 같이 종료
    timer_check_thread.start()
    
    try:
        print(f"Listening for messages on topic '{TOPIC_SUBSCRIBE}'...")
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n프로그램 종료...")
    finally:
        disconnect_future = mqtt_connection.disconnect()
        disconnect_future.result()
        led.off()
        timer.stop()
        print("연결 종료!")
