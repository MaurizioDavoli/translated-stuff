"""tool to get simple the operations with the front api"""
import time
from datetime import datetime, timedelta
import datetime
from pytz import timezone

import urllib.parse

import requests

ACCESS_TOKEN = None

url = "https://api2.frontapp.com/conversations/search/"

headers = {}

# offers_tag = ['automatic',  'forwarded',  'dragged_ui', 'maybe_offer']
# offers_tag = ['tag_1dxwxy', 'tag_1g44qu', 'tag_1j5rie', 'tag_1j5rra']
offers_tag = ['tag_1dxwxy', 'tag_1g44qu', 'tag_1j5rie']


def _get_date_range(start_date, date_range):
    """:return a touple of start and end date"""
    init_date = datetime.datetime.timestamp(start_date)
    end_date = datetime.datetime.timestamp(start_date + timedelta(days=date_range))
    return init_date, end_date


def _create_url(start, end, tag, inbox):
    """:return encoded url for get request"""
    start_query = " after:" + str(start)
    end_query = " before:" + str(end)
    tag_query = ""
    if tag is not None:
        tag_query = " tag:" + tag
    inbox_query = ""
    if inbox is not None:
        inbox_query = " inbox:" + inbox
    query = start_query + end_query + tag_query + inbox_query
    return url + urllib.parse.quote(query)


def _query_to_api(query_url):
    """:return json result to get query at a given url"""
    query_response = None
    while query_response is None:
        try:
            query_response = requests.request("GET", query_url, headers=headers).json()
            if '_error' in query_response:
                print(query_response)
                time.sleep(1)
                query_response = None
        except Exception as error:
            query_response = None
            print(error)
    return query_response


def _parse_tag(tag_list):
    parsed_list = []
    for row in tag_list:
        parsed_list.append(row['name'])
    return parsed_list


def _parse_contact(contact_list):
    parsed_list = []
    for row in contact_list:
        parsed_list.append((row['source'],
                            row['handle']))
    return parsed_list


def _from_timestampms_to_datetime_cet(timestamp_ms):
    utc_dt = datetime.datetime.fromtimestamp(timestamp_ms).replace(microsecond=0)
    arrival_date = utc_dt.astimezone(timezone('Europe/Rome')).replace(tzinfo=None)
    return arrival_date


class FrontUtility:

    def __init__(self, token):
        global ACCESS_TOKEN
        ACCESS_TOKEN = token
        global headers
        headers = {
            "Accept": "application/json",
            "Authorization": "Bearer " + ACCESS_TOKEN
        }

    @staticmethod
    def get_conversations(start_date, day_range=1, tag=None, inbox=None):
        dates = _get_date_range(start_date, day_range)
        query_url = _create_url(dates[0], dates[1], tag, inbox)
        searched_data = {}
        first_page = True
        while query_url is not None:
            query_response = _query_to_api(query_url)
            print(query_url)
            if first_page:
                searched_data = query_response
                first_page = False
            else:
                searched_data['_results'].extend(query_response['_results'])
            if query_url != query_response['_pagination']['next']:
                query_url = query_response['_pagination']['next']
            else:
                raise RecursionError

        return searched_data

    def get_yesterday_conversations(self, tag=None):
        yesterday = datetime.datetime.now() - timedelta(days=1)
        # TODO: CHANGE THIS!!!! IS JUST FOR TESTS SAKE
        yesterday_ok = datetime.datetime(yesterday.year, yesterday.month, yesterday.day).replace(month=11)
        to_check_conversations = {}
        try:
            response = self.get_conversations(yesterday_ok, tag=tag)
            to_check_conversations = response['_results']
            print(response['_total'])
        except RecursionError:
            print("API ERROR")
        return to_check_conversations

    @staticmethod
    def get_contact(contact_url):
        response = _query_to_api(contact_url)['handles']
        if response is None:
            return []
        return response

    def get_parsed_yesterday_conversation(self, tag=None):
        conversations = self.get_yesterday_conversations(tag=tag)
        parsed_list = []
        print("still working.. it may take a bit")
        a = 0
        for row in conversations:
            parsed_list.append((row['id'],
                                row['subject'],
                                row['status'],
                                _parse_tag(row['tags']),
                                _from_timestampms_to_datetime_cet(row['created_at']),
                                _parse_contact(self.get_contact(row['recipient']['_links']['related']['contact']))
                                ))
        return parsed_list

    def get_tagged_yesterday_parsed_conversations(self):
        parsed_list = []
        for tag in offers_tag:
            conversations = self.get_parsed_yesterday_conversation(tag=tag)
            for conv in conversations:
                parsed_list.append(conv)
        return parsed_list
