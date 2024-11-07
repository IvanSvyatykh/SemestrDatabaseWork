from pathlib import Path
import numpy as np
import pandas as pd

from generate_data import (
    generate_aircraft_number_history,
    generate_passengers_flight_info_df,
    generate_status_history,
)


def __write_data_to_csv(
    data: pd.DataFrame, csv_name: str, path_to_res_dir: Path
) -> None:

    data.to_csv(
        path_to_res_dir / csv_name,
        index=False,
    )


def __prepare_passenger(path_to_csv: Path, path_to_res_dir: Path) -> None:
    passenger_data = pd.read_csv(path_to_csv)
    passenger_id = passenger_data["passenger_id"]
    passenger_name = passenger_data["passenger_name"]
    passenger_data = passenger_data.drop("passenger_name", axis=1)
    passenger_data = passenger_data.drop("passenger_id", axis=1)
    passenger_data["name"] = passenger_name.apply(
        lambda x: x.split(" ")[0].strip(" ")
    )
    passenger_data["surname"] = passenger_name.apply(
        lambda x: x.split(" ")[-1].strip(" ")
    )
    passenger_data["passport_ser"] = passenger_id.apply(
        lambda x: x.split(" ")[0].strip(" ")
    )
    passenger_data["passport_num"] = passenger_id.apply(
        lambda x: x.split(" ")[-1].strip(" ")
    )
    __write_data_to_csv(passenger_data, path_to_csv.name, path_to_res_dir)


def __prepare_fare_cond(path_to_csv: Path, path_to_res_dir: Path) -> None:
    fare_cond_data = pd.read_csv(path_to_csv)
    fare_cond_data["id"] = [i + 1 for i in range(len(fare_cond_data))]
    __write_data_to_csv(fare_cond_data, path_to_csv.name, path_to_res_dir)


def __get_aircraft_number_story(
    id_series: pd.Series, airlines: pd.DataFrame
) -> pd.DataFrame:
    result = pd.DataFrame()
    for id in id_series:
        result = pd.concat(
            [
                result,
                generate_aircraft_number_history(
                    id,
                    pattern=r"^[A-Z]-[A-Z]{4}|[A-Z]{2}-[A-Z]{3}|N[0-9]{3}[A-Z]{3}$",
                    min_year=2000,
                    max_year=2015,
                    airlines=airlines,
                ),
            ]
        )
    return result


def __prepare_aircrafts(path_to_csv: Path, path_to_res_dir: Path) -> None:
    airafts_data = pd.read_csv(path_to_csv)
    airafts_data.rename(
        columns={"aircraft_code": "iata_name", "model": "name"},
        inplace=True,
    )
    airafts_data = pd.concat(
        [airafts_data, airafts_data.sample(frac=0.7)], ignore_index=True
    )
    airafts_data["id"] = [i + 1 for i in range(len(airafts_data))]
    aircraft_number_story = __get_aircraft_number_story(
        airafts_data["id"], pd.read_csv(path_to_res_dir / "airlines.csv")
    )
    __write_data_to_csv(
        aircraft_number_story, "aircraft_number.csv", path_to_res_dir
    )
    __write_data_to_csv(airafts_data, path_to_csv.name, path_to_res_dir)


def __prepare_airports(path_to_csv: Path, path_to_res_dir: Path) -> None:
    airports_data = pd.read_csv(path_to_csv)
    __write_data_to_csv(airports_data, path_to_csv.name, path_to_res_dir)


def __prepare_airlines(path_to_csv: Path, path_to_res_dir: Path) -> None:
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


def __prepare_flights(path_to_csv: Path, path_to_res_dir: Path) -> None:

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
    __write_data_to_csv(
        generate_status_history(schedules_data),
        "status_history.csv",
        path_to_res_dir,
    )
    schedules_data = schedules_data.drop(columns="status")
    __write_data_to_csv(
        generate_passengers_flight_info_df(
            flights_data,
            schedules_data,
            pattern=r"^[A-Z]{2}[0-9]{1,2}|[A-Z]{1}[0-9]{1,2}$",
        ),
        "passenger_flight_info.csv",
        path_to_res_dir,
    )
    __write_data_to_csv(statuses_data, "statuses.csv", path_to_res_dir)
    __write_data_to_csv(schedules_data, "schedules.csv", path_to_res_dir)
    __write_data_to_csv(flights_data, path_to_csv.name, path_to_res_dir)


def __prepare_tickets(path_to_csv: Path, path_to_res_dir: Path) -> None:

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
    tickets_data["baggage"] = np.round(
        np.random.normal(10, 2.5, len(tickets_data))
    )
    tickets_data["is_regisred"] = np.random.binomial(
        n=1, p=0.9, size=len(tickets_data)
    ).astype(bool)
    __write_data_to_csv(tickets_data, path_to_csv.name, path_to_res_dir)


FUNCTION = {
    "fare_condition": __prepare_fare_cond,
    "passengers": __prepare_passenger,
    "airlines": __prepare_airlines,
    "airports": __prepare_airports,
    "aircrafts": __prepare_aircrafts,
    "flights": __prepare_flights,
    "tickets": __prepare_tickets,
}
