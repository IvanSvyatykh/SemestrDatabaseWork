import pandas as pd
from pathlib import Path
import datetime

from mongodb.repository import *
from schemas import *


async def __load_airports(csv_path: Path) -> None:

    airports_df = pd.read_csv(csv_path)
    oids = []
    airport_repository = AirportRepository()
    for _, row in airports_df.iterrows():
        airport = Airport(
            iata_name=row["airport_code"],
            airport_name=row["airport_name"],
            city=row["city"],
            timezone=row["timezone"],
        )
        oids.append(str(await airport_repository.add(airport)))
    airports_df["oid"] = oids
    airports_df.to_csv(csv_path, index=False)


async def __load_schedules(csv_path: Path) -> None:

    schedules_df = pd.read_csv(csv_path)
    oids = []
    schedule_repository = ScheduleRepository()
    for _, row in schedules_df.iterrows():
        schedule = Schedule(
            arrival_time=datetime.datetime.fromisoformat(
                str(row["scheduled_arrival"])
            ),
            departure_time=datetime.datetime.fromisoformat(
                str(row["scheduled_departure"])
            ),
            actual_arrival=datetime.datetime.fromisoformat(
                str(row["actual_arrival"])
            ),
            actual_departure=datetime.datetime.fromisoformat(
                str(row["actual_departure"])
            ),
        )
        oids.append(str(await schedule_repository.add(schedule)))
    schedules_df["oid"] = oids
    schedules_df.to_csv(csv_path, index=False)


async def __load_statuses(csv_path: Path) -> None:
    statuses_df = pd.read_csv(csv_path)
    oids = []
    status_repository = StatusRepositiry()

    for _, row in statuses_df.iterrows():
        status = Status(status=row["status"])
        oids.append(str(await status_repository.add(status)))

    statuses_df["oid"] = oids
    statuses_df.to_csv(csv_path, index=False)


async def __load_status_history(csv_path: Path) -> None:
    status_histories_df = pd.read_csv(csv_path)
    statuses_df = pd.read_csv(csv_path.parent / "statuses.csv")
    schedules_df = pd.read_csv(csv_path.parent / "schedules.csv")
    oids = []
    status_history_repository = StatusHistoryRepository()
    for _, row in status_histories_df.iterrows():
        status_history = StatusHistory(
            status_id=statuses_df[statuses_df["id"] == row["status_id"]][
                "oid"
            ].iloc[0],
            schedule_id=schedules_df[
                schedules_df["id"] == row["schedule_id"]
            ]["oid"].iloc[0],
            set_status_time=datetime.datetime.fromisoformat(
                str(row["start_time"])
            ),
            unset_status_time=datetime.datetime.fromisoformat(
                str(row["end_time"])
            ),
        )
        oids.append(
            str(await status_history_repository.add(status_history))
        )
    status_histories_df["oid"] = oids
    status_histories_df.to_csv(csv_path, index=False)


async def __load_airlines(csv_path: Path) -> None:
    airlines_df = pd.read_csv(csv_path)
    oids = []
    airline_repository = AirlineRepository()

    for _, row in airlines_df.iterrows():
        airline = Airline(name=row["name"], iata_name=row["iata_name"])
        oids.append(str(await airline_repository.add(airline)))
    airlines_df["oid"] = oids
    airlines_df.to_csv(csv_path, index=False)


async def __load_aircraft(csv_path: Path) -> None:
    aircrafts_df = pd.read_csv(csv_path)
    oids = []
    aircraft_repository = AircraftRepository()

    for _, row in aircrafts_df.iterrows():
        aircraft = Aircraft(
            iata_name=row["iata_name"],
            aircraft_name=row["name"],
            seats_num=row["count"],
        )
        oids.append(str(await aircraft_repository.add(aircraft)))

    aircrafts_df["oid"] = oids
    aircrafts_df.to_csv(csv_path, index=False)


async def __load_aircraft_numbers(csv_path: Path) -> None:
    aircraft_numbers_df = pd.read_csv(csv_path)
    aircrafts_df = pd.read_csv(csv_path.parent / "aircrafts.csv")
    airlines_df = pd.read_csv(csv_path.parent / "airlines.csv")
    oids = []
    aircraft_number_repository = AircraftNumberRepository()

    for _, row in aircraft_numbers_df.iterrows():
        aircraft_number = AircraftNumber(
            aircraft_id=aircrafts_df[
                aircrafts_df["id"] == row["aircraft_id"]
            ]["oid"].iloc[0],
            aircraft_num=row["aircraft_num"],
            airline=airlines_df[
                airlines_df["iata_name"] == row["iata_airlines"]
            ]["oid"].iloc[0],
            registration_time=datetime.datetime.fromisoformat(
                str(row["registration_date"])
            ),
            derigistration_time=datetime.datetime.fromisoformat(
                str(row["deregistration_date"])
            ),
        )
        oids.append(
            str(await aircraft_number_repository.add(aircraft_number))
        )
    aircraft_numbers_df["oid"] = oids
    aircraft_numbers_df.to_csv(csv_path, index=False)


async def __load_flights(csv_path: Path) -> None:

    flights_df = pd.read_csv(csv_path)
    aircrafts_df = pd.read_csv(csv_path.parent / "aircrafts.csv")
    schedules_df = pd.read_csv(csv_path.parent / "schedules.csv")
    airports_df = pd.read_csv(csv_path.parent / "airports.csv")
    cargo_flight_infos_df = None
    passenger_flight_infos_df = None

    passenger_flight_info_path = (
        csv_path.parent / "passenger_flight_info.csv"
    )
    cargo_flight_info_path = csv_path.parent / "cargo_flight_info.csv"

    if cargo_flight_info_path.exists():
        cargo_flight_infos_df = pd.read_csv(cargo_flight_info_path)

    if passenger_flight_info_path.exists():
        passenger_flight_infos_df = pd.read_csv(passenger_flight_info_path)

    flight_repository = FlightRepositiry()
    oids = []
    for _, row in flights_df.iterrows():
        info = None
        if row["is_passenger"] and passenger_flight_infos_df is not None:
            info = PassengerFlightInfo(
                gate=passenger_flight_infos_df[
                    passenger_flight_infos_df["flight_id"]
                    == row["flight_id"]
                ]["gate"].iloc[0],
                is_ramp=passenger_flight_infos_df[
                    passenger_flight_infos_df["flight_id"]
                    == row["flight_id"]
                ]["is_ramp"].iloc[0],
                registration_time=passenger_flight_infos_df[
                    passenger_flight_infos_df["flight_id"]
                    == row["flight_id"]
                ]["registration_time"].iloc[0],
            )
        else:
            continue

        flight = Flight(
            flight_number=row["flight_no"],
            aircraft=aircrafts_df[
                aircrafts_df["iata_name"] == row["aircraft_code"]
            ]["oid"].iloc[0],
            arrival_airport=airports_df[
                airports_df["airport_code"] == row["arrival_airport"]
            ]["oid"].iloc[0],
            departure_airport=airports_df[
                airports_df["airport_code"] == row["departure_airport"]
            ]["oid"].iloc[0],
            schedule=schedules_df[schedules_df["id"] == row["schedule"]][
                "oid"
            ].iloc[0],
            info=info,
        )
        oids.append(str(await flight_repository.add(flight)))
    flights_df["oid"] = oids
    flights_df.to_csv(csv_path)


async def __load_passengers(csv_path: Path) -> None:

    passengers_df = pd.read_csv(
        csv_path,
        dtype={"passport_ser": str, "passport_num": str},
    )
    oids = []
    passenger_repository = PassengerRepository()

    for _, row in passengers_df.iterrows():

        passenger = Passenger(
            name=row["name"],
            surname=row["surname"],
            passport_ser=row["passport_ser"],
            passport_num=row["passport_num"],
        )
        oids.append(str(await passenger_repository.add(passenger)))

    passengers_df["oid"] = oids
    passengers_df.to_csv(csv_path, index=False)


async def __load_fair_conds(csv_path: Path) -> None:

    fair_conds_df = pd.read_csv(csv_path)
    oids = []
    repository = FairCondRepository()
    for _, row in fair_conds_df.iterrows():
        fair_cond = FairCondition(fare_condition=row["fare_conditions"])
        oids.append(str(await repository.add(fair_cond)))
    fair_conds_df["oid"] = oids
    fair_conds_df.to_csv(csv_path, index=False)


async def __load_tickets(csv_path: Path) -> None:

    tickets_df = pd.read_csv(
        csv_path,
        dtype={"passport_ser": str, "passport_num": str, "ticket_no": str},
    )
    passengers_df = pd.read_csv(
        csv_path.parent / "passengers.csv",
        dtype={"passport_ser": str, "passport_num": str},
    )
    flights_df = pd.read_csv(csv_path.parent / "flights.csv")
    fair_conds_df = pd.read_csv(csv_path.parent / "fare_condition.csv")
    oids = []
    ticket_repository = TicketRepository()

    for _, row in tickets_df.iterrows():
        ticket = Ticket(
            passenger=passengers_df[
                (passengers_df["passport_ser"] == row["passport_ser"])
                & (passengers_df["passport_num"] == row["passport_num"])
            ]["oid"].iloc[0],
            fare_condition=fair_conds_df[
                fair_conds_df["id"] == row["fare_conditions"]
            ]["oid"].iloc[0],
            flight=flights_df[flights_df["flight_id"] == row["flight_id"]][
                "oid"
            ].iloc[0],
            number=row["ticket_no"],
            cost=row["amount"],
            baggage_weight=row["baggage"],
            is_registred=row["is_regisred"],
            seat_num=row["seat_no"],
        )
        oids.append(str(await ticket_repository.add(ticket)))
    tickets_df["oid"] = oids
    tickets_df.to_csv(csv_path, index=False)


LOAD_FUNCTIONS = {
    "airports": __load_airports,
    "schedule": __load_schedules,
    "status": __load_statuses,
    "status_history": __load_status_history,
    "airline": __load_airlines,
    "aircraft": __load_aircraft,
    "aircraft_number": __load_aircraft_numbers,
    "fair_cond": __load_fair_conds,
    "passenger": __load_passengers,
    "flight": __load_flights,
    "ticket": __load_tickets,
}
