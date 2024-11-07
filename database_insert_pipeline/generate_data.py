import datetime
import os
import random
from zoneinfo import ZoneInfo
from bson import ObjectId
import numpy as np
import rstr
import pandas as pd


def __generate_dates_for_flight_num(
    min_year: int,
    max_year: int,
    timezone: str,
) -> list[datetime.datetime]:

    res = []
    count = 0
    step = max(3, (max_year - min_year) // 3)
    for i in range(min_year, max_year, step):
        year = random.randint(i, i + step)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        date = datetime.datetime(
            year,
            month,
            day,
            hour=0,
            minute=0,
            second=0,
            tzinfo=ZoneInfo(timezone),
        )
        if count > 0:
            res.append(date - datetime.timedelta(days=1))
        res.append(date)
        count += 1
    res.append(
        datetime.datetime(
            year=9999,
            month=12,
            day=31,
            hour=23,
            minute=59,
            second=59,
            tzinfo=ZoneInfo(timezone),
        )
    )
    return res


def __generate_flight_number(pattern: str, aircraft_nums: list) -> str:
    while True:
        number = rstr.xeger(string_or_regex=pattern)

        if number not in aircraft_nums:
            return number


def generate_aircraft_number_history(
    aircraft_id: str,
    pattern: str,
    min_year: int,
    airlines: pd.DataFrame,
    max_year: int = 2015,
    timezone: str = "Asia/Yekaterinburg",
) -> pd.DataFrame:

    dates = __generate_dates_for_flight_num(min_year, max_year, timezone)

    temp_dic = {}
    temp_dic["aircraft_id"] = []
    temp_dic["aircraft_num"] = []
    temp_dic["registration_date"] = []
    temp_dic["deregistration_date"] = []
    temp_dic["iata_airlines"] = []
    for i in range(1, len(dates), 2):

        temp_dic["aircraft_id"].append(str(aircraft_id))
        temp_dic["aircraft_num"].append(
            __generate_flight_number(
                pattern=pattern, aircraft_nums=temp_dic["aircraft_num"]
            )
        )
        temp_dic["iata_airlines"].append(
            airlines.sample(n=1)["iata_name"].iloc[0]
        )
        temp_dic["registration_date"].append(dates[i - 1])
        temp_dic["deregistration_date"].append(dates[i])

    return pd.DataFrame(temp_dic)


def __generate_registration_time(
    schedules_data: pd.DataFrame, schedule_id: int
) -> datetime.datetime:
    scheduled_arrival = datetime.datetime.fromisoformat(
        schedules_data[schedules_data["id"] == schedule_id][
            "scheduled_arrival"
        ].iloc[-1]
    )
    return scheduled_arrival - datetime.timedelta(hours=2)


def generate_passengers_flight_info_df(
    flights_data: pd.DataFrame, schedules_data: pd.DataFrame, pattern: str
) -> pd.DataFrame:
    info = {}
    info["flight_id"] = flights_data["flight_id"]
    info["gate"] = [
        rstr.xeger(string_or_regex=pattern)
        for i in range(len(flights_data))
    ]
    info["is_ramp"] = np.random.binomial(
        n=1, p=0.9, size=len(flights_data)
    ).astype(bool)
    info["registration_time"] = [
        __generate_registration_time(schedules_data, id)
        for id in flights_data["schedule"]
    ]
    return pd.DataFrame(info)


def generate_status_history(schedule_data: pd.DataFrame) -> pd.DataFrame:
    result = {}
    result["status_id"] = []
    result["schedule_id"] = []
    result["start_time"] = []
    result["end_time"] = []
    for _, row in schedule_data.iterrows():
        result["status_id"].append(row["status"])
        result["schedule_id"].append(row["id"])
        result["start_time"].append(row["scheduled_departure"])
        result["end_time"].append(row["scheduled_arrival"])
    return pd.DataFrame(result)
