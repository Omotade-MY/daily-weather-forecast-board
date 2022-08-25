
import pandas as pd
from data_api import gen_state

from process import ParseFile, to_csv


def process():

    state = pd.read_csv("list_of_capitals.csv").dropna()
    capitals = state['Capital'].values

    # Extract
    for data in gen_state(capitals):
    
        # Transform
        parser = ParseFile(json_handler=data)

        city = parser.parse_area()
        weather, astronomy, hourly  = parser.parse_weather()

        # Load (to csv)
        to_csv('city.csv',city.dict())
        to_csv('weather.csv', weather.dict())
        to_csv('astronomy.csv', astronomy.dict())
        to_csv('metadata.csv', hourly)



# Execute
process()
