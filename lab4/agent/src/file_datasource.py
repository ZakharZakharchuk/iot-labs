from csv import DictReader
from datetime import datetime
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.aggregated_data import AggregatedData
from domain.parking import Parking
import config


class FileDatasource:
    def __init__(self, accelerometer_filename: str, gps_filename: str, parking_filename: str) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        self.accel_file = None
        self.gps_file = None
        self.parking_file = None
        self.agr_data = None

    def read(self) -> AggregatedData:
        try:
            return self.agr_data.pop(0)
        except IndexError:
            return AggregatedData.default()

    def startReading(self):
        self.accel_file = open(self.accelerometer_filename, "r")
        self.gps_file = open(self.gps_filename, "r")
        self.parking_file = open(self.parking_filename, "r")

        accel_reader = DictReader(self.accel_file, delimiter=",")
        gps_reader = DictReader(self.gps_file, delimiter=",")
        parking_reader = DictReader(self.parking_file, delimiter=",")

        accel_data = [
            Accelerometer(int(row["x"]), int(row["y"]), int(row["z"]))
            for row in accel_reader
        ]

        gps_data = [
            Gps(float(row["latitude"]), float(row["longitude"]))
            for row in gps_reader
        ]

        parking_data = [
            Parking(int(row["empty_count"]), Gps(float(row["latitude"]), float(row["longitude"])))
            for row in parking_reader
        ]

        self.agr_data = [
            AggregatedData(
                accel,
                gps if gps is not None else Gps(0.0, 0.0),
                parking if parking is not None else Parking(0, Gps(0.0, 0.0)),
                datetime.now(),
                config.USER_ID,
            )
            for accel, gps, parking in zip(
                accel_data,
                gps_data + [None] * (len(accel_data) - len(gps_data)),
                parking_data + [None] * (len(accel_data) - len(parking_data)),
            )
        ]
        print("Successfully opened files")

    def stopReading(self):
        if self.accel_file:
            print("Closing file...", self.accel_file.name)
            self.accel_file.close()
        if self.gps_file:
            print("Closing file...", self.gps_file.name)
            self.gps_file.close()
        if self.parking_file:
            print("Closing file...", self.parking_file.name)
            self.parking_file.close()
