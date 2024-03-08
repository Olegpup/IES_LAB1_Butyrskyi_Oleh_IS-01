from datetime import datetime
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.parking import Parking 
from domain.aggregated_data import AggregatedData
import config


class FileDatasource:
    def __init__(
        self,
        accelerometer_filename: str,
        gps_filename: str,
        parking_filename: str,
    ) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        self.accelerometer_file = None
        self.gps_file = None
        self.parking_file = None

    def get_next_data(self, data_file):
        try:
            data_row = next(data_file)
        except StopIteration:
            data_file.seek(0)
            next(data_file)
            data_row = next(data_file)

        return data_row.strip().split(',')

    def read(self) -> AggregatedData:
        if not all([self.accelerometer_file, self.gps_file, self.parking_file]):
            raise ValueError("Files not initialized.")

    
        accelerometer_data = self.get_next_data(self.accelerometer_file)
        gps_data = self.get_next_data(self.gps_file)
        parking_raw_data = self.get_next_data(self.parking_file)
        parking_data = [
            parking_raw_data[2],
            Gps(parking_raw_data[0], parking_raw_data[1])
            ]
        
        return AggregatedData(
            Accelerometer(*accelerometer_data),
            Gps(*gps_data),
            Parking(*parking_data),
            datetime.now(),
            config.USER_ID
        )
    

    def startReading(self, *args, **kwargs):
        self.accelerometer_file = open(self.accelerometer_filename, 'r')
        self.gps_file = open(self.gps_filename, 'r')
        self.parking_file = open(self.parking_filename, 'r')
        next(self.accelerometer_file)
        next(self.gps_file)
        next(self.parking_file)


    def stopReading(self, *args, **kwargs):
        if self.accelerometer_file:
            self.accelerometer_file.close()
        if self.gps_file:
            self.gps_file.close()
        if self.parking_file:
            self.parking_file.close()
