import asyncio
import json
from threading import Thread
import lineMapLayer

from kivy.app import App
from kivy.clock import Clock
from kivy_garden.mapview import MapMarker, MapView, MapLayer
from kivy.uix.label import Label
import fileDatasource
import websockets


class MapViewApp(App):
    server_uri = "ws://localhost:8000/ws/1"

    def __init__(self):
        super().__init__()
        self.lineMapLayer = lineMapLayer.LineMapLayer()
        self.car_marker = MapMarker()
        self.car_label = Label()
        self.road_state = None



    def on_start(self):
        """
        Встановлює необхідні маркери, викликає функцію для оновлення мапи
        """
        speed_bump_cor = fileDatasource.get_bump_cor()
        pothole_cor = fileDatasource.get_pothole_cor()

        for _, point in pothole_cor.iterrows():
            self.set_pothole_marker(point)

        for _, point in speed_bump_cor.iterrows():
            self.set_bump_marker(point)

        self.start_websocket_listener()

    def start_websocket_listener(self):
        websocket_thread = Thread(target=self.run_websocket_client, daemon=True)
        websocket_thread.start()

    def run_websocket_client(self):
        """
        Клієнт WebSocket, що працює в окремому потоці.
        Виконує асинхронний метод `connect_to_websocket` у циклі подій asyncio.
        """
        asyncio.run(self.connect_to_websocket())

    async def connect_to_websocket(self):
        """
        Асинхронний клієнт WebSocket, який отримує оновлення в режимі реального часу.
        """
        try:
            async with websockets.connect(self.server_uri, ping_interval=300, ping_timeout=100) as websocket:
                print(f"Connected to {self.server_uri}")
                while True:
                    message = await websocket.recv()
                    self.process_websocket_message(message)
        except Exception as e:
            print(f"WebSocket connection error: {e}")

    def process_websocket_message(self, message):
        """
        Обробляє вхідне повідомлення та планує оновлення маркера в основному потоці Kivy.
        :param message: Рядок JSON від WebSocket, наприклад: {"lat": 50.4475, "lon": 30.4520}
        """
        try:
            data = json.loads(json.loads(message))
            self.road_state = data.get("road_state")
            agent_data = data.get("agent_data")
            point = agent_data.get("gps")
            lat = point.get("latitude")
            lon = point.get("longitude")
            if lat is not None and lon is not None:
                Clock.schedule_once(lambda dt: self.update_car_marker(lat, lon))
        except json.JSONDecodeError:
            print(f"Invalid message format: {message}")

    def update_car_marker(self, lat, lon):
        """
        Оновлює відображення маркера машини на мапі
        :param point: GPS координати
        """

        self.mapview.remove_marker(self.car_marker)
        self.car_marker = MapMarker(lat=lat, lon=lon, source="images/car.png")
        self.mapview.add_marker(self.car_marker)

    def update_label_position(self, dt):
        """Updates label position when map moves/zooms."""
        if self.car_marker and self.car_label and self.road_state:
            x, y = self.mapview.get_window_xy_from(
                self.car_marker.lat,
                self.car_marker.lon,
                self.mapview.zoom
            )
            self.car_label.pos = x - 60, y - 30
            self.car_label.text = self.road_state

    def set_pothole_marker(self, point):
        """
        Встановлює маркер для ями
        :param point: GPS координати
        """
        pothole_marker = MapMarker(lat=float(point[0]), lon=float(point[1]), source='images/pothole.png')
        self.mapview.add_marker(pothole_marker)

    def set_bump_marker(self, point):
        """
        Встановлює маркер для лежачого поліцейського
        :param point: GPS координати
        """
        bump_marker = MapMarker(lat=float(point[0]), lon=float(point[1]), source='images/bump.png')
        self.mapview.add_marker(bump_marker)

    def build(self):
        """
        Ініціалізує мапу MapView(zoom, lat, lon)
        :return: мапу
        """
        self.mapview = MapView(zoom=15, lat=50.4474, lon=30.45176)

        self.car_label = Label(
            size_hint=(None, None),
            size=(120, 40),
            color=(1, 0, 0, 1),
        )

        self.mapview.add_widget(self.car_label)
        self.label_update_event = Clock.schedule_interval(self.update_label_position, 0.001)
        return self.mapview


if __name__ == '__main__':
    mapp = MapViewApp()
    mapp.build()
    mapp.run()
