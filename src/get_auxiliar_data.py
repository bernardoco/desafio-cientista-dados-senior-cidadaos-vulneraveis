import requests

import pandas as pd
import geopandas
from shapely import wkt


class GetData():
    def generate_meteo_url(coordinates: tuple, start: str, end: str, **params):
        BASE_URL = "https://historical-forecast-api.open-meteo.com"
        version = "v1"
        params = '&'.join(params)
        url = f"{BASE_URL}/{version}/forecast?latitude={coordinates['latitude']}&longitude={coordinates['longitude']}&{params}&start_date={start}&end_date={end}"
        return url


    def generate_holidays_url(year: str):
        BASE_URL = "https://date.nager.at/api"
        version = "v3"
        url = f"{BASE_URL}/{version}/PublicHolidays/{year}/BR"
        return url


    def request_data(url: str) -> pd.DataFrame:
        r = requests.get(url)
        return pd.json_normalize(r.json())


if __name__ == "__main__":
    get_data = GetData()
    
    # Get holidays data
    years = ["2023", "2024"]
    holidays = pd.DataFrame()
    for year in years:
        url = get_data.generate_holidays_url(year)
        holidays = pd.concat([holidays, get_data.request_data(url)])


    # Get meteo data
    params = ["weather_code", "temperature_2m_max", "precipitation_sum", "wind_speed_10m_max", "uv_index_max", "temperature_2m_min"]
    start_date = "2023-01-01"
    end_date = "2024-12-31"

    df = geopandas.read_file("../data/bairro.csv")
    df['geometry'] = df['geometry'].apply(wkt.loads)
    gdf = geopandas.GeoDataFrame(df, crs="EPSG:4326")
    gdf = gdf.set_index("id_bairro")
    gdf["centroid"] = gdf.to_crs('+proj=cea').centroid.to_crs(gdf.crs)

    coordinates = {}
    for id_bairro in gdf.index:
        coordinates[id_bairro] = {
            "longitude": gdf["centroid"].loc[id_bairro].x,
            "latitude": gdf["centroid"].loc[id_bairro].y
        }

    for id_bairro in gdf.index:
        url = get_data.generate_meteo_url(coordinates[id_bairro], start_date, end_date, params)
        meteo_df = get_data.request_data(url)

        cols = [f"daily.{param}" for param in params]
        cols.append("daily.time")
        meteo_df = meteo_df.explode(cols)
        meteo_df = meteo_df.reset_index(drop=True)

        meteo_df["id_bairro"] = id_bairro
        meteo_df.to_csv(f"../data/meteo/{id_bairro}.csv")