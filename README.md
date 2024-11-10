# Структура проекта

```
├── data
│   ├── aircrafts.csv
│   ├── airlines.csv
│   ├── airports.csv
│   ├── data.zip
│   ├── fare_condition.csv
│   ├── flights.csv
│   ├── passengers.csv
│   ├── runway_condition.csv
│   └── tickets.csv
├── database
│   ├── config.py
│   ├── documents.py
│   ├── __init__.py
│   └── repository.py
├── database_insert_pipeline
│   ├── config.yaml
│   ├── data_loader.py
│   ├── generate_data.py
│   ├── insert_script.py
│   ├── load_data.py
│   ├── prepare_data.py
├── docker-compose.yaml
├── notebooks
│   ├── refacror_flight.ipynb
│   ├── refactor_aircrafts.ipynb
│   ├── refactor_airlines.ipynb
│   ├── refactor_airports.ipynb
│   ├── refactor_fare_condition.ipynb
│   ├── refactor_passenger.ipynb
│   ├── refactor_schedule.ipynb
│   └── refactor_tickets.ipynb
├── poetry.lock
├── pyproject.toml
├── README.md
├── src
│   ├── main.py
│   └── schemas.py
└── ytsaurus
    ├── init.sh
    ├── start_YTsaurus.sh
    ├── stop_YTsaurus.sh
    └── YTsaurus_cluster.yaml

```
- docker-compose.yaml - Файл с инструкцией для запуска docker compose.
- data - Дирректория с данными для заполнения бд.
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
- MongoExpress - это веб клиент для работы с MongoDB, так как бесплатные версии программ для работы с СУБД не поддерживают NoSQL.

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

# Заполнения БД

Архив с данными можно скачать по [ссылке](https://disk.yandex.ru/d/bfcM9RSuw-ipJA). 

Для заполнения бд тестовыми данными, необходимо перейти в корень:
- Настроить переменные окружения в .env по примеру из `.env.example`
- `mkdir ./data`
- `cd ./data`
- `unzip db_semest_job_data.zip`
- `cd ..`
- `cd ./database_insert_pipeline`
- Ввести команду в консоль `python insert_script.py -c config.yaml -rs <some_int>`

