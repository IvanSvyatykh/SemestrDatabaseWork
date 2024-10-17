from pydantic import ValidationError
from .documents import PassengerDocument, PassportDocument
from server.schemas import Passenger
from mongoengine import Q, QuerySet
from mongoengine.fields import ObjectId


class PassengerRepository:

    def __init__(self):
        self.passenger = PassengerDocument()

    def __is_exist(
        self, pass_ser: str, pass_num: str
    ) -> PassengerDocument:
        passenger_query_set = PassengerDocument.objects(
            Q(passport__number=pass_num) & Q(passport__series=pass_ser)
        )
        if len(passenger_query_set) > 1:
            raise ValueError(
                "Database return more than one passenger with this passport"
            )
        return passenger_query_set.first()

    def add(self, passenger: Passenger) -> None:
        passenger_db_document = self.__is_exist(
            passenger.passport_ser, passenger.passport_num
        )
        if passenger_db_document is not None:
            raise ValueError("Person with those passport already exist")

        self.passenger.name = passenger.name
        self.passenger.surname = passenger.surname
        self.passenger.passport = PassportDocument(
            series=passenger.passport_ser,
            number=passenger.passport_num,
        )
        self.passenger.save()

    def delete(self, passenger: Passenger) -> None:
        passenger_db_document = self.__is_exist(
            passenger.passport_ser, passenger.passport_num
        )
        if passenger_db_document is not None:
            raise ValueError("Person with those passport already exist")
        passenger_db_document.delete()

    def update(self, passenger: Passenger) -> None:
        passenger_db_document = self.__is_exist(
            passenger.passport_ser, passenger.passport_num
        )
        if passenger_db_document is not None:
            raise ValueError("Person with those passport already exist")
        passenger_db_document.update(
            set__name=passenger.name,
            set__surname=passenger.surname,
            set__passport=PassportDocument(
                series=passenger.passport_ser,
                number=passenger.passport_num,
            ),
        )

    def get_by_id(self, oid: ObjectId) -> Passenger:
        passenger_db_document = PassengerDocument.objects(id=oid).first()
        if passenger_db_document is None:
            raise ValueError("There is not person with these ObjectId")

        return Passenger(
            name=passenger_db_document.name,
            surname=passenger_db_document.surname,
            passport_num=passenger_db_document.passport.number,
            passport_ser=passenger_db_document.passport.series,
        )

    def get_by_passport(
        self, pass_series: str, pass_num: str
    ) -> Passenger:
        passenger_db_document = self.__is_exist(pass_series, pass_num)
        if len(passenger_db_document) == 0:
            raise ValueError("There is not person with these passport")

        return Passenger(
            name=passenger_db_document.name,
            surname=passenger_db_document.surname,
            passport_num=passenger_db_document.passport.number,
            passport_ser=passenger_db_document.passport.series,
        )
