"""collection of method to get silmple the operation with front"""
import logging
import time
from datetime import timedelta
import datetime
import urllib.parse

import requests


FRONT_API_SEARCH_PATH = "https://api2.frontapp.com/conversations/search/"


def _get_date_range(start_date, date_range):
    """:return a touple of start and end date"""
    init_date = datetime.datetime.timestamp(start_date)
    end_date = datetime.datetime.timestamp(start_date + timedelta(hours=date_range))
    return init_date, end_date


def _create_url(start, end, tag, inbox):
    """:return encoded url for get request"""
    query_dict = {'after': str(start),
                  'before': str(end),
                  'tag': tag,
                  'inbox': inbox}
    query_to_encode = ''.join([' %s:%s' % (k, v) for k, v in query_dict.items() if v is not None])
    return FRONT_API_SEARCH_PATH + urllib.parse.quote(query_to_encode)


class FrontApiUtility:

    access_token = None
    front_api_request_headers = {}

    def __init__(self, token):
        self.access_token = token
        self.front_api_request_headers = {
            "Accept": "application/json",
            "Authorization": "Bearer " + self.access_token
        }

    def get_query_to_api(self, query_url):
        """:return json result to get query at a given url"""
        query_response = None
        try:
            query_response = requests.request("GET", query_url, headers=self.front_api_request_headers).json()
            time.sleep(2)
            if '_error' in query_response:
                logging.error(query_response)
                query_response = None
                time.sleep(60)
        except Exception as error:
            logging.error(error)
        return query_response

    def get_one_day_conversations(self, start_date, day_range=1, tag=None, inbox=None):
        """:return a list of conversation with a date as input"""
        dates = _get_date_range(start_date, day_range)
        query_url = _create_url(dates[0], dates[1], tag, inbox)
        searched_data = {}
        first_page = True
        while query_url is not None:
            query_response = self.get_query_to_api(query_url)
            if query_response:
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

    def get_last_week_conversations(self, tag=None, inbox=None):
        """:return the conversations of one day one week ago"""
        last_week = datetime.datetime.now() - timedelta(days=7)
        last_week_ok = datetime.datetime(last_week.year,
                                         last_week.month,
                                         last_week.day,
                                         last_week.hour,
                                         last_week.minute)
        last_week_conversations = {}
        response = self.get_one_day_conversations(last_week_ok, tag=tag, inbox=inbox)
        if response:
            last_week_conversations = response['_results']
            logging.info(response['_total'])
        return last_week_conversations
