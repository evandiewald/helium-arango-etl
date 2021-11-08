import time

import aiohttp.client_exceptions
import requests
import asyncio
from aiohttp import ClientSession
from datetime import datetime
import itertools
import json


def get_hotspots(limit=None):
    url = 'https://api.helium.io/v1/hotspots'
    hotspot_list = []
    while True:
        res = requests.get(url).json()
        hotspot_list += res['data']
        if limit and len(hotspot_list) > limit:
            hotspot_list = hotspot_list[:limit]
            break
        try:
            url = 'https://api.helium.io/v1/hotspots?cursor=' + res['cursor']
        except KeyError:
            break
        if len(hotspot_list) % 20e3 == 0:
            print(len(hotspot_list))
    for hotspot in hotspot_list:
        hotspot['_key'] = hotspot['address']
    return hotspot_list


def get_witnesses_for_hotspot(hotspot_address: str):
    url = f'https://api.helium.io/v1/hotspots/{hotspot_address}/witnesses'
    witnesses = requests.get(url).json()['data']
    return witnesses



async def fetch_witnesses(address, session):
    url = 'https://api.helium.io/v1/hotspots/{}/witnesses'.format(address)
    async with session.get(url) as response:
        # backoff = 1
        # while True:
        #     try:
        r = await response.read()
            #     if backoff > 128:
            #         raise Exception('Backoff limit exceeded.')
            # except aiohttp.client_exceptions.ContentTypeError:
            #     print('Backing off for {} seconds'.format(backoff))
            #     time.sleep(backoff)
            #     backoff *= 2
    edges = []
    try:
        data = json.loads(r.decode('utf-8'))
        for hotspot in data['data']:
            edges.append({'_from': address, '_to': hotspot['address'], 'last_updated': datetime.utcnow().isoformat()})
    except json.decoder.JSONDecodeError:
        print(r)
    return edges


async def list_witnesses_async(address_list: list):
    tasks = []
    # Fetch all responses within one Client session,
    # keep connection alive for all requests.

    async with ClientSession() as session:
        for address in address_list:
            task = asyncio.ensure_future(fetch_witnesses(address, session))
            tasks.append(task)
        responses = await asyncio.gather(*tasks)
        # you now have all response bodies in this variable
    return responses


def create_witness_edges_for_address_list(address_list: list):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(list_witnesses_async(address_list))
    loop.run_until_complete(future)
    results = future.result()
    return list(itertools.chain.from_iterable(results))