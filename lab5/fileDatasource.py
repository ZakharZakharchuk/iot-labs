import pandas as pd
from matplotlib import pyplot as plt
from scipy.signal import find_peaks

acc_data = pd.read_csv('data.csv')
cor_data = pd.read_csv('acc_cor.csv')
z_data = acc_data['Z'].values

speed_bump_params = {
    'height': 20000,
    'distance': 20,
    'prominence': 500,
    'width': 10
}

pothole_params = {
    'height': -10000,
    'distance': 20,
    'prominence': 500,
    'width': 3
}


def get_bump_cor():
    speed_bumps, speed_bump_properties = find_peaks(z_data, **speed_bump_params)
    return cor_data.iloc[[-(len(speed_bumps))]]


def get_pothole_cor():
    # Пошук ям (вузькі западини)
    potholes, pothole_properties = find_peaks(-z_data, **pothole_params)
    return cor_data.head(len(potholes))


def build_graph():
    speed_bumps, speed_bump_properties = find_peaks(z_data, **speed_bump_params)
    potholes, pothole_properties = find_peaks(-z_data, **pothole_params)
    plt.figure(figsize=(14, 7))
    plt.plot(z_data, label='Z дані', color='steelblue')
    plt.scatter(speed_bumps, z_data[speed_bumps], color='orange', label='Лежачі поліцейські', zorder=3)
    plt.scatter(potholes, z_data[potholes], color='purple', label='Ями', zorder=3)
    plt.axhline(y=16667, color='green', linestyle='--', label='Стан спокою (16667 одиниць)')
    plt.title('Виявлення лежачих поліцейських та ям')
    plt.xlabel('Часова позначка')
    plt.ylabel('Значення Z (одиниці)')
    plt.legend()
    plt.grid(True)
    plt.show()


if __name__ == '__main__':
    print(len(get_bump_cor()))