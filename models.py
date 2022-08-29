from pydantic import BaseModel

class City(BaseModel):
    name: str
    region: str
    country: str
    latitude: float
    longitude: float
    utcOffset: float = 1.0
    zone: str = "Africa/Lagos"
    


class Astronomy(BaseModel):
    city: str = ""
    date: str = ""
    sunrise: str
    sunset: str
    moonrise: str
    moonset: str
    moon_phase: str
    moon_illumination: int
    
    
class Weather(BaseModel):
    city: str = ""
    date: str = ""  
    maxtempC: float
    maxtempF: float
    mintempC: float
    mintempF: float
    avgtempC: float
    avgtempF: float
    totalSnow_cm: float
    sunHour: float
    uvIndex: float
    