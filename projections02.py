# force projections in CH
    # <allow_experimental_projection_optimization>1</allow_experimental_projection_optimization>
    # <force_optimize_projection>1</force_optimize_projection>
# export TOKEN=
# tb workspace clear --yes
# push the transformation matview
    # tb push datasources/analytics_events.datasource
    # tb push pipes/analytics_hits_mv.pipe
# start ingesting random data
# python generate.py $TOKEN stream
# create the projections over the `analytics_hits` data source
# python projections02.py $TOKEN
# tb push --push-deps
# start the dashboard project and all queries and widgets should work


import sys
from telnetlib import TTYLOC
from urllib.parse import urlencode

import requests


def create_projections(token):
    result = requests.get(f'http://localhost:8001/v0/datasources/analytics_hits?token={token}')
    table_id = result.json()['id']

    counts = '''
        select
          date,
          referrer,
          pathname,
          location,
          device,
          browser,
          uniq(session_id) as visits,
          count() as hits
        group by
          date,
          referrer,
          pathname,
          location,
          device,
          browser
    '''
    create_projection(table_id, 'counts', counts, materialize=True)

    sessions = '''
        select
            date,
            session_id,
            any(device) as device,
            any(browser) as browser,
            any(location) as location,
            min(timestamp) as first_hit,
            max(timestamp) as latest_hit,
            uniq(session_id) visits,
            count() as hits
        group by
            date,
            session_id
        '''
    create_projection(table_id, 'sessions', sessions, materialize=True)

    trend = '''
        select
            toStartOfMinute(timestamp) t,
            uniq(session_id) as visits
        group by
            t
        '''
    create_projection(table_id, 'trend', trend, materialize=True)

def create_projection(table_id, projection_name, query, materialize=False):
    params = {
        'query': f"select database, name from system.tables where name = '{table_id}' FORMAT JSON"
    }
    result = requests.get(f'http://localhost:8123?{urlencode(params)}')
    database = result.json()['data'][0]['database']

    params = {
        'query': f'alter table {database}.{table_id} add projection {projection_name} ({query});',
    }

    params_drop = {
        'query': f'alter table {database}.{table_id} drop projection {projection_name};',
    }

    params_mutations = {
        'query': f"kill mutation where table = '{table_id}' SYNC;"
    }

    print(f'kill_mutations {table_id}')
    result = requests.post(f'http://localhost:8123?{urlencode(params_mutations)}')
    print(f'drop projection {projection_name}')
    result = requests.post(f'http://localhost:8123?{urlencode(params_drop)}')
    print(f'create projection {projection_name}')
    result = requests.post(f'http://localhost:8123?{urlencode(params)}')
    print(result.status_code)

    if materialize:
        print(f'materialize projection {projection_name}')

        params = {
            'query': f'alter table {database}.{table_id} materialize projection {projection_name};'
        }
        result = requests.post(f'http://localhost:8123?{urlencode(params)}')
        print(result.status_code)


if __name__ == "__main__":
    create_projections(sys.argv[1])

