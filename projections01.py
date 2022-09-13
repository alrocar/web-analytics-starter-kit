import sys
from telnetlib import TTYLOC
from urllib.parse import urlencode

import requests


def create_projections(token):
    result = requests.get(f'http://localhost:8001/v0/datasources/analytics_events?token={token}')
    table_id = result.json()['id']

    base = '''
        WITH lower(JSONExtractString(payload, 'user-agent')) as user_agent
        SELECT
        timestamp,
        action,
        version,
        coalesce(session_id,'0') as session_id,
        JSONExtractString(payload, 'locale') as locale,
        JSONExtractString(payload, 'location') as location,
        JSONExtractString(payload, 'referrer') as referrer,
        JSONExtractString(payload, 'pathname') as pathname,
        JSONExtractString(payload, 'href') as href,
        case
            when match(user_agent, 'wget|ahrefsbot|curl|urllib|bitdiscovery|\+https://|googlebot') then 'bot'
            when match(user_agent, 'android') then 'mobile-android'
            when match(user_agent, 'ipad|iphone|ipod') then 'mobile-ios'
            else 'desktop'
        END as device,
        case
            when match(user_agent, 'firefox') then 'firefox'
            when match(user_agent, 'chrome|crios') then 'chrome'
            when match(user_agent, 'opera') then 'opera'
            when match(user_agent, 'msie|trident') then 'ie'
            when match(user_agent, 'iphone|ipad|safari') then 'safari'
            else 'Unknown'
        END as browser
        FROM
        analytics_events
    '''

    pages = '''
        WITH lower(JSONExtractString(payload, 'user-agent')) as user_agent
        SELECT
            uniq(session_id) visits,
            count() hits,
            toDate(timestamp) date,
            action,
            JSONExtractString(payload, 'location') as location,
            JSONExtractString(payload, 'pathname') as pathname,
            case
                when match(user_agent, 'firefox') then 'firefox'
                when match(user_agent, 'chrome|crios') then 'chrome'
                when match(user_agent, 'opera') then 'opera'
                when match(user_agent, 'msie|trident') then 'ie'
                when match(user_agent, 'iphone|ipad|safari') then 'safari'
                else 'Unknown'
            END as browser
        GROUP BY
            date, action, location, pathname, browser
    '''
    create_projection(table_id, 'pages', pages, materialize=True)

    sources = '''
        WITH lower(JSONExtractString(payload, 'user-agent')) as user_agent
        SELECT
            uniq(coalesce(session_id,'0')) visits,
            count() hits,
            toDate(timestamp) date,
            action,
            JSONExtractString(payload, 'location') as location,
            JSONExtractString(payload, 'referrer') as referrer,
            case
                when match(user_agent, 'wget|ahrefsbot|curl|urllib|bitdiscovery|\+https://|googlebot') then 'bot'
                when match(user_agent, 'android') then 'mobile-android'
                when match(user_agent, 'ipad|iphone|ipod') then 'mobile-ios'
                else 'desktop'
            END as device,
            case
                when match(user_agent, 'firefox') then 'firefox'
                when match(user_agent, 'chrome|crios') then 'chrome'
                when match(user_agent, 'opera') then 'opera'
                when match(user_agent, 'msie|trident') then 'ie'
                when match(user_agent, 'iphone|ipad|safari') then 'safari'
                else 'Unknown'
            END as browser
        GROUP BY
            date, action, location, referrer, device, browser
    '''
    create_projection(table_id, 'sources', sources, materialize=True)

    sessions = '''
        WITH
            lower(JSONExtractString(payload, 'user-agent')) as user_agent,
            JSONExtractString(payload, 'location') as _location,
            case
                when match(user_agent, 'wget|ahrefsbot|curl|urllib|bitdiscovery|\+https://|googlebot') then 'bot'
                when match(user_agent, 'android') then 'mobile-android'
                when match(user_agent, 'ipad|iphone|ipod') then 'mobile-ios'
                else 'desktop'
            END as _device,
            case
                when match(user_agent, 'firefox') then 'firefox'
                when match(user_agent, 'chrome|crios') then 'chrome'
                when match(user_agent, 'opera') then 'opera'
                when match(user_agent, 'msie|trident') then 'ie'
                when match(user_agent, 'iphone|ipad|safari') then 'safari'
                else 'Unknown'
            END as _browser
        SELECT
            anySimpleState(_device) as device,
            anySimpleState(_browser) as browser,
            anySimpleState(_location) as location,
            minSimpleState(timestamp) as first_hit,
            maxSimpleState(timestamp) as latest_hit,
            session_id,
            count() hits,
            toDate(timestamp) date,
            action
        GROUP BY
            date, action, session_id
    '''
    create_projection(table_id, 'sessions', sessions, materialize=True)

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
