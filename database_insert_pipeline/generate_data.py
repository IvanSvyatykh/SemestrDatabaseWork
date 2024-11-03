import datetime
import os
import random
from zoneinfo import ZoneInfo
from bson import ObjectId
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


def generate_aircraft_number_story(
    aircraft_id: ObjectId,
    pattern: str,
    min_year: int,
    max_year: int = 2015,
    timezone: str = "Asia/Yekaterinburg",
) -> pd.DataFrame:

    dates = __generate_dates_for_flight_num(min_year, max_year, timezone)

    temp_dic = {}
    temp_dic["aircraft_id"] = []
    temp_dic["aircraft_num"] = []
    temp_dic["registration_date"] = []
    temp_dic["deregistration_date"] = []
    for i in range(1, len(dates), 2):

        temp_dic["aircraft_id"].append(str(aircraft_id))
        temp_dic["aircraft_num"].append(
            rstr.xeger(string_or_regex=pattern)
        )
        temp_dic["registration_date"].append(dates[i - 1])
        temp_dic["deregistration_date"].append([dates[i]])

    return pd.DataFrame(temp_dic)
