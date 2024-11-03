from pathlib import Path
import re

import pandas as pd


def __write_data_to_csv(
    data: pd.DataFrame, csv_name: str, path_to_res_dir: Path
) -> None:

    data.to_csv(
        path_to_res_dir / csv_name,
        index=False,
    )


def __normalize_passenger(
    path_to_csv: Path, path_to_res_dir: Path
) -> None:
    passenger_data = pd.read_csv(path_to_csv)
    passenger_id = passenger_data["passenger_id"]
    passenger_data = passenger_data.drop("passenger_id", axis=1)
    passenger_data["passpor_ser"] = passenger_id.apply(
        lambda x: x.split(" ")[0].strip(" ")
    )
    passenger_data["passpor_num"] = passenger_id.apply(
        lambda x: x.split(" ")[-1].strip(" ")
    )
    __write_data_to_csv(passenger_data, path_to_csv.name, path_to_res_dir)


def __normalize_fare_cond(
    path_to_csv: Path, path_to_res_dir: Path
) -> None:
    fare_cond_data = pd.read_csv(path_to_csv)
    fare_cond_data["id"] = [i + 1 for i in range(len(fare_cond_data))]
    __write_data_to_csv(fare_cond_data, path_to_csv.name, path_to_res_dir)


def __normalize_aircrafts(
    path_to_csv: Path, path_to_res_dir: Path
) -> None:
    airafts_data = pd.read_csv(path_to_csv)
    __write_data_to_csv(airafts_data, path_to_csv.name, path_to_res_dir)


def __normalize_airports(path_to_csv: Path, path_to_res_dir: Path) -> None:
    airports_data = pd.read_csv(path_to_csv)
    __write_data_to_csv(airports_data, path_to_csv.name, path_to_res_dir)


def __normalize_airlines(path_to_csv: Path, path_to_res_dir: Path) -> None:
    airlines_data = pd.read_csv(path_to_csv)
    airlines_data = airlines_data.dropna()
    airlines_data = airlines_data[
        airlines_data.IATA.str.contains(r"^[A-Z]{2}$")
    ]

    airlines_data = airlines_data[airlines_data["Country"] == "Russia"]
    res = pd.DataFrame()
    res["name"] = airlines_data["Name"]
    res["iata_name"] = airlines_data["IATA"]
    airlines_data = res
    __write_data_to_csv(airlines_data, path_to_csv.name, path_to_res_dir)


def __get_schedule_df(
    flights_data: pd.DataFrame, schedule_columns: list
) -> tuple[pd.DataFrame, list]:
    schedule_temp_data = {}

    schedule_temp_data["id"] = []

    for column in schedule_columns:
        schedule_temp_data[column] = []

    schedules_id = []
    for i, row in flights_data.iterrows():
        schedules_id.append(i + 1)
        schedule_temp_data["id"].append(i + 1)
        for column in schedule_columns:
            schedule_temp_data[column].append(row[column])

    return pd.DataFrame(schedule_temp_data), schedules_id


def __get_statuses_df(schedules_data: pd.DataFrame) -> pd.DataFrame:
    statuses_temp_data = {}
    statuses_temp_data["id"] = [
        i + 1 for i in range(len(schedules_data["status"].unique()))
    ]

    statuses_temp_data["status"] = schedules_data["status"].unique()
    return pd.DataFrame(statuses_temp_data)


def __normalize_flights(path_to_csv: Path, path_to_res_dir: Path) -> None:

    schedule_columns = [
        "scheduled_arrival",
        "scheduled_departure",
        "actual_departure",
        "actual_arrival",
        "status",
    ]

    flights_data = pd.read_csv(path_to_csv)
    flights_data["is_passenger"] = [True for i in range(len(flights_data))]
    schedules_data, schedules_id = __get_schedule_df(
        flights_data, schedule_columns
    )
    flights_data.drop(labels=schedule_columns, axis=1, inplace=True)
    flights_data["schedule"] = schedules_id
    statuses_data = __get_statuses_df(schedules_data)
    schedules_data["status"] = schedules_data["status"].apply(
        lambda x: statuses_data[statuses_data["status"] == x]["id"].iloc[0]
    )
    __write_data_to_csv(statuses_data, "statuses.csv", path_to_res_dir)
    __write_data_to_csv(schedules_data, "schedules.csv", path_to_res_dir)
    __write_data_to_csv(flights_data, path_to_csv.name, path_to_res_dir)


def __normalize_tickets(path_to_csv: Path, path_to_res_dir: Path) -> None:

    tickets_data = pd.read_csv(path_to_csv)
    passenger_id = tickets_data["passenger_id"]
    tickets_data = tickets_data.drop("passenger_id", axis=1)
    tickets_data["passpor_ser"] = passenger_id.apply(
        lambda x: x.split(" ")[0].strip(" ")
    )
    tickets_data["passpor_num"] = passenger_id.apply(
        lambda x: x.split(" ")[-1].strip(" ")
    )
    fare_cond_data = pd.read_csv("../data/normalized/fare_condition.csv")
    tickets_data["fare_conditions"] = tickets_data[
        "fare_conditions"
    ].apply(
        lambda x: fare_cond_data[fare_cond_data["fare_conditions"] == x][
            "id"
        ].iloc[0]
    )
    __write_data_to_csv(tickets_data, path_to_csv.name, path_to_res_dir)


FUNCTION = {
    "passengers": __normalize_passenger,
    "fare_condition": __normalize_fare_cond,
    "aircrafts": __normalize_aircrafts,
    "airports": __normalize_airports,
    "airlines": __normalize_airlines,
    "flights": __normalize_flights,
    "tickets": __normalize_tickets,
}
