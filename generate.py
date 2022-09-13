from faker import Faker
from faker.providers import user_agent, misc, date_time
import requests
from datetime import datetime
import locale
import json
import sys


def gen_event(fake, locales, token=None, this_year=False):
    return {
        "timestamp": datetime.now().isoformat() if token and not this_year else fake.date_time_this_year().isoformat(),
        "session_id": fake.uuid4(),
        "payload": json.dumps({
            "user-agent": fake.user_agent(),
            "locale": fake.random_element(elements=locales),
            "location": fake.random_element(elements=('location_a', 'location_b', 'location_c', 'location_d')),
            "referrer": fake.random_element(elements=('referrer_a', 'referrer_b', 'referrer_c', 'referrer_d')),
            "pathname": fake.random_element(elements=('pathname_a', 'pathname_b', 'pathname_c', 'pathname_d')),
            "href": fake.random_element(elements=('href_a', 'href_b', 'href_c', 'href_d')),
        }),
        "version": fake.random_element(elements=('1', '2', '3', '4')),
        "action": fake.random_element(elements=['page_hit']),
    }


def gen_events(fake, locales, token=None, this_year=False):
    with open("output/analytics_events.json", "w") as file:
        for i in range(NUM_ROWS):
            print(str(i))
            event = gen_event(fake, locales, token=token, this_year=this_year)
            if token:
                params = {
                    'name': 'analytics_events',
                    'token': token,
                }
                result = requests.post(f'http://localhost:8042/v0/events', params=params, data=json.dumps(event))
                print(result.json())
            else:
                json.dump(event, file)
                file.write("\n")


if __name__ == "__main__":
    locales = list(locale.locale_alias.keys())
    DATE_RANGE = 500
    NUM_ROWS = 10000000000

    args = sys.argv
    token = None
    this_year = False
    if len(args) > 1:
        token = args[1]
        this_year = args[2] if len(args) > 2 else False

    faker = Faker()
    faker.add_provider(user_agent)
    faker.add_provider(misc)
    faker.add_provider(date_time)

    gen_events(faker, locales, token=token, this_year=this_year)
