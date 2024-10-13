# Структура проета

```
├── data
├── database_insert_pipeline
│   ├── config.yaml
│   ├── data_loader.py
│   └── main.py
├── docker-compose.yaml
├── notebooks
│   └── refactor_passenger.ipynb
├── poetry.lock
├── pyproject.toml
├── README.md
└── src
    ├── database
    │   ├── config.py
    │   ├── documents
    │   │   ├── aircrafts_document.py
    │   │   ├── airline_document.py
    │   │   ├── airport_document.py
    │   │   ├── flight_document.py
    │   │   ├── __init__.py
    │   │   ├── passenger_document.py
    │   │   ├── runway_document.py
    │   │   ├── schedule_document.py
    │   │   ├── seatclasses_document.py
    │   │   ├── status_document.py
    │   │   ├── ticket_document.py
    │   │   └── weather_document.py
    │   ├── __init__.py
    │   └── service
    │       └── passenger_service.py
    ├── main.py
    └── server
        ├── api
        │   └── __init__.py
        ├── app.py
        ├── __init.py
        └── schemas
            ├── __init__.py
            ├── passenger.py

```
- docker-compose.yaml - Файл с инструкцией для запуска docker compose.
- data - Дирректория с данными для заполнения бд
- database_insert_pipline - Дирректория со скриптом для заполнения бд.
- notebooks - Дирректория с ноутбуками, для обработки данных, так как выгрузка данных в csv файл из сторонних истоников, не всегда позволяет сразу добавлять из в бд без предобрабоки.
- src - Дирректория со всеми файлами для реализации безнес логики.
- src/database - Дирректория с файлами для подключения к бд, схемами данных.
- src/server(не заню как правильно назвать) - Дирректория с реализацией бизнес логики.
- poetry.lock - Файл с зависимостями.
- poetry.toml - Файл с информацией о проете.


# Запуск docker-compose

В корне проекта находиться файл `docker-compose.yaml`. В нем находиться инструкция для запуска: 
- MongoDB
- MongoExpress - это веб клиент для работы с MongoDB, так как бесплатные версии программ для работы с СУБД, не поддерживают NoSQL.

Также в корне проекта находиться файл `.env.example`, в нем записаны все необходимые переменные окружения для успешного запуска docker-compose, а также работы самого приложения. Пояснения к переменным:
- MONGO_ROOT_USER - это имя супер-пользователя для бд.
- MONGO_ROOT_PASSWORD - это пароль от суперпользователя бд.
- MONGOEXPRESS_LOGIN - это логин для подключения к веб-клиенту для работы с MongoDB.
- MONGOEXPRESS_PASSWORD - это пароль от веб-клиента
- DOMAIN - это домен в котором работает.
- PORT - это порт который слушает MongoDB, необходим для указания его в строке подключения к бд.

Для запуска docker-compose  ввести в командную строку, находясь в одной с ним директории, следующую команду `docker compose up -d`

# Подключение к веб-клиенту

Для подключение к веб-клиенту, в браузере перейте по URL `http://localhost:8081`

# Заполнения БД(Не готово, архива нет)

Для заполнения бд тестовыми данными, необходимо воспользоваться скриптом находящимся в папке `database_insert_pipeline`, для заполнения бд, необходимо перейти в папку `database_insert_pipeline`, а также заполнить пути до файлов с данными в  файл `config.yaml`.

Архив с данными можно скачать по [ссылке]().