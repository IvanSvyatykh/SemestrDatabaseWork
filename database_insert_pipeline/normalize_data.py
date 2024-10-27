from pathlib import Path

import pandas as pd


def __write_data_to_csv(data: pd.DataFrame, path_to_csv: Path) -> None:

    data.to_csv(
        path_to_csv=path_to_csv.parent
        / (path_to_csv.stem + "_normalized.csv"),
        index=False,
    )


def __normalize_passenger(path_to_csv: Path) -> None:
    passenger_data = pd.read_csv(path_to_csv)
    passenger_id = passenger_data["passenger_id"]
    passenger_data = passenger_data.drop("passenger_id", axis=1)
    passenger_data["passpor_ser"] = passenger_id.apply(
        lambda x: x.split(" ")[0].strip(" ")
    )
    passenger_data["passpor_num"] = passenger_id.apply(
        lambda x: x.split(" ")[-1].strip(" ")
    )
    __write_data_to_csv(passenger_data, path_to_csv)


FUNCTION = {"passenger": __normalize_passenger}
