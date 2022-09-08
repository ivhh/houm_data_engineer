from typing import Any
from numpy import NaN
import pandas as pd

from weather.api import WeatherApi
from datetime import datetime
from schemas.properties import dtypes as prop_dtype
from schemas.users import dtypes as users_dtype
from schemas.visits import dtypes as visits_dtype


import os


def to_timestamp_bins(datestr: str, afterhr: bool) -> int:
    """Return the corresponding Epoch timestamp truncating or ceiling to the nearest
    hour block

    Args:
        datestr (str): string date in format %Y-%m-%dT%H:%M:%S%z
        afterhr (bool): wether is aproximating truncating (True) or not

    Returns:
        int: Epoch time as an integer
    """

    ts = int(datetime.strptime(datestr, "%Y-%m-%dT%H:%M:%S%z").timestamp())
    diff = ts % 3600 if not afterhr else -3600 + (ts % 3600)
    return ts - diff


def filter_api_data(api_data: Any) -> Any:
    """Filtering data from API response"""

    # Not likely, but it could be on different days
    visit_days = {}

    for data in api_data:
        day = data["days"][0]
        if not visit_days.get(day["datetime"]):
            visit_days[day["datetime"]] = {}
            visit_days[day["datetime"]]["temp"] = 0
            visit_days[day["datetime"]]["bins"] = 0
            visit_days[day["datetime"]]["avg_day_temp"] = day["temp"]
            visit_days[day["datetime"]]["rainy_day"] = (
                day.get("preciptype") is not None and "rain" in day["preciptype"]
            )
            visit_days[day["datetime"]]["datetime"] = day["datetime"]
            visit_days[day["datetime"]]["temp_available"] = True

        # Sometimes the current condition is not available for a given hour
        # in case is not then its going to be ommited. If the is none available
        # its going to be imputed by the day average (not the best approach)

        if data.get("currentConditions") and data["currentConditions"].get("temp"):
            visit_days[day["datetime"]]["temp"] += data["currentConditions"]["temp"]
        else:
            visit_days[day["datetime"]]["temp_available"] = False
        visit_days[day["datetime"]]["bins"] += 1

    for visit_day in visit_days.values():
        if visit_day["temp_available"]:
            visit_day["temp"] = visit_day["temp"] / visit_day["bins"]
        else:
            visit_day["temp"] = visit_day["avg_day_temp"]

    return {"api": list(visit_days.values())}


def main():
    """Script to get data requested and answering the questions of
    this test"""

    # Definition of important variables

    DS_PROPERTIES_PATH = "datasets/properties.csv"
    DS_USERS_PATH = "datasets/users.csv"
    DS_VISITS_PATH = "datasets/visits.csv"
    WEATHER_API_KEY = os.environ.get("WEATHER_API_KEY")

    # Read files dataframes

    properties_df = pd.read_csv(DS_PROPERTIES_PATH, dtype=prop_dtype)
    users_df = pd.read_csv(DS_USERS_PATH, dtype=users_dtype)
    visits_df = pd.read_csv(DS_VISITS_PATH, dtype=visits_dtype, parse_dates=True)

    # Init weather API

    api = WeatherApi(WEATHER_API_KEY)

    # Process data to answer target questions
    # to do it the following variables are defined
    #
    #   overall_visits: Number of total visits on dataset
    #   avg_prop_owned: Average properties owned by owners
    #   avg_temp_prop_2: Average temperature of visit days fulfilled on property with id 2
    #   avg_temp_rain_visit: Average temperature for visit days fulfilled on rainy days
    #   avg_temp_suba_visit: Average temperature for visit days fulfilled on Suba

    overall_visits = 0
    avg_prop_owned = 0
    avg_temp_prop_2 = 0
    avg_temp_rain_visit = 0
    avg_temp_suba_visit = 0

    # To get information of avg_temp_* variables, weather information of all visits
    # tagged as done needs to be retrived

    visits_tmp_df = visits_df[visits_df.status == "Done"].merge(
        properties_df, how="inner", on="property_id"
    )
    visits_tmp_df = visits_tmp_df.merge(users_df, how="inner", on="property_id")
    visits_tmp_df["location"] = (
        visits_tmp_df["latitude"] + "," + visits_tmp_df["longitude"]
    )
    visits_tmp_df["begin_date"] = visits_tmp_df.apply(
        lambda x: to_timestamp_bins(x["begin_date"], False), axis=1
    )
    visits_tmp_df["end_date"] = visits_tmp_df.apply(
        lambda x: to_timestamp_bins(x["end_date"], True), axis=1
    )

    # Here I could try to minimize the calls of the API grouping by location
    # and checking only the intervals needed (to avoid repeating a call). But
    # as the reads are less than the 1000 given by the API, it is not needed

    visits_tmp_df = visits_tmp_df[
        [
            "scheduled_id",
            "property_id",
            "user_id",
            "localidad",
            "location",
            "begin_date",
            "end_date",
        ]
    ]

    api_data = {}

    # Not recommended to use iterrows but in this case is needed

    for _, row in visits_tmp_df.iterrows():
        api_data[row["scheduled_id"]] = filter_api_data(
            api.get_weather_timeline(
                row["location"], row["begin_date"], row["end_date"], hourly=True
            )
        )

    api_data_df = (
        pd.DataFrame.from_dict(api_data, orient="index")
        .reset_index(drop=False)
        .rename(columns={"index": "scheduled_id"})
        .explode("api")
    )
    api_data_df = pd.concat(
        [api_data_df.drop(["api"], axis=1), api_data_df["api"].apply(pd.Series)],
        axis=1,
    )
    api_data_df = api_data_df.merge(visits_tmp_df, how="inner", on="scheduled_id")

    # The groupby is used in this case if there are multiple days on one visits
    # I know it is not likely and even forbidden by business rules

    user2_prop_gb = api_data_df[api_data_df["user_id"] == 2].groupby("scheduled_id")
    suba_prop_gb = api_data_df[api_data_df["localidad"] == "Suba"].groupby(
        "scheduled_id"
    )
    rainy_prop_gb = api_data_df[api_data_df["rainy_day"] == True].groupby(
        "scheduled_id"
    )

    def my_agg(x):
        names = {"w_avg_temp": (x["temp"] * x["bins"]).sum() / x["bins"].sum()}
        return pd.Series(names, index=["w_avg_temp"])

    # Average temperature in Celsius
    temp_avg = (
        lambda g: (g.apply(my_agg).mean()["w_avg_temp"] - 32) * 5 / 9
        if len(g) > 0
        else NaN
    )

    overall_visits = len(visits_tmp_df)
    avg_prop_owned = users_df.groupby("user_id")["user_id"].count().mean()
    avg_temp_prop_2 = temp_avg(user2_prop_gb)
    avg_temp_rain_visit = temp_avg(suba_prop_gb)
    avg_temp_suba_visit = temp_avg(rainy_prop_gb)

    print("\nResultados \n")
    print(f"Total de visitas realizadas: {overall_visits}")
    print(f"Promedio de propiedades por propietario: {avg_prop_owned}")
    print(
        f"Promedio de temperatura visitas realizadas en propiedad con ID 2: {avg_temp_prop_2} °C"
    )
    print(
        f"Promedio de temperatura de visitas para días con lluvia: {avg_temp_rain_visit} °C"
    )
    print(
        f"Promedio de temperatura de visitas realizadas en Suba: {avg_temp_suba_visit} °C"
    )
    print("\n")


if __name__ == "__main__":
    main()
