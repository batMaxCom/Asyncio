# Запуск проекта Asyncio:

Создать файл .env по примеру:
```
PG_DB=name_db
PG_USER=user
PG_PASSWORD=passwoed
```
Создать БД через Docker-compose:
```
docker-compose up -d
```
Запуск программы:
```
python swappy_async.py
```