from kivy.app import App
from kivy_garden.mapview import MapMarker, MapView
from kivy.clock import Clock
from datasource import Datasource
from lineMapLayer import LineMapLayer

class MapViewApp(App):
    def __init__(self, **kwargs):
        super().__init__()
        self.datasource = Datasource(user_id=1)  # Используем тестовый user_id

    def on_start(self):
        """Инициализирует карту и запускает обновление данных."""
        self.map_layer = LineMapLayer()
        self.mapview.add_layer(self.map_layer, mode="scatter")
        Clock.schedule_interval(self.update, 1)  # Обновляем каждую секунду

    def update(self, *args):
        """Получает новые точки и обновляет отображение карты."""
        new_points = self.datasource.get_new_points()
        for point in new_points:
            self.update_car_marker(point)

    def update_car_marker(self, point):
        """Обновляет маркер машины на карте."""
        latitude, longitude, road_state = point
        marker = MapMarker(lat=latitude, lon=longitude)
        self.mapview.add_widget(marker)

    def set_pothole_marker(self, point):
        """Устанавливает маркер для ямы."""
        latitude, longitude, _ = point
        marker = MapMarker(lat=latitude, lon=longitude, source="images/pothole.png")
        self.mapview.add_widget(marker)

    def set_bump_marker(self, point):
        """Устанавливает маркер для лежачего полицейского."""
        latitude, longitude, _ = point
        marker = MapMarker(lat=latitude, lon=longitude, source="images/bump.png")
        self.mapview.add_widget(marker)

    def build(self):
        """Создает и возвращает объект карты."""
        self.mapview = MapView(zoom=10, lat=50.45, lon=30.52)
        return self.mapview

if __name__ == '__main__':
    MapViewApp().run()
