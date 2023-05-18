import asyncio
import datetime
from pprint import pprint

import aiohttp
from asyncio import run
from more_itertools import chunked
from models import SwapiPeople, engine, Base, Session
from sqlalchemy import select

MAX_REQUEST = 5
DB_FIELD = [column.key for column in SwapiPeople.__table__.columns]


async def get_info(client, link, field):
    async with client.get(link) as responce:
        json_data = await responce.json()
        return json_data[field]


async def get_people(client, people_id):
    async with client.get(f"http://swapi.dev/api/people/{people_id}") as responce:
        if responce.status == 200:
            json_data = await responce.json()
            films_coros = [get_info(client, link, 'title') for link in json_data['films']]
            species_coros = [get_info(client, link, 'name') for link in json_data['species']]
            starships_coros = [get_info(client, link, 'name') for link in json_data['starships']]
            vehicles_coros = [get_info(client, link, 'name') for link in json_data['vehicles']]
            json_data['id'] = people_id
            json_data['films'] = ','.join(await asyncio.gather(*films_coros))
            json_data['species'] = ','.join(await asyncio.gather(*species_coros))
            json_data['starships'] = ','.join(await asyncio.gather(*starships_coros))
            json_data['vehicles'] = ','.join(await asyncio.gather(*vehicles_coros))

            json_data = {key: value for (key, value) in json_data.items() if key in DB_FIELD}
            return json_data


async def paste_to_db(people_jsons):
    async with Session() as session:
        orm_object = [SwapiPeople(**item) for item in people_jsons if item is not None]
        session.add_all(orm_object)
        await session.commit()


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    async with aiohttp.ClientSession() as client:
        for people_id_chunk in chunked(range(80, 90), MAX_REQUEST):
            person_coros = [get_people(client, people_id) for people_id in people_id_chunk]
            result = await asyncio.gather(*person_coros)
            paste_to_db_coro = paste_to_db(result)
            paste_to_db_task = asyncio.create_task(paste_to_db_coro)
            tasks = asyncio.all_tasks() - {asyncio.current_task(), }
            for task in tasks:
                await task

async def print_all_objects():
    async with Session() as session:
        result = await session.execute(select(SwapiPeople))
        pprint([res.to_dict() for res in result.scalars().all()])

if __name__ == '__main__':
    start = datetime.datetime.now()
    run(main())
    print(datetime.datetime.now()-start)
    # run(print_all_objects())


