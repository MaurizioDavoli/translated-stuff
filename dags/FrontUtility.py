"""tool to get simple the operations with the front api"""
import time
from datetime import datetime, timedelta
import datetime
import urllib.parse

import requests

ACCESS_TOKEN = None

url = "https://api2.frontapp.com/"

headers = {}


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
    return url + "conversations/search/" + urllib.parse.quote(query)


def _query_to_api(query_url):
    """:return json result to get query at a given url"""
    query_response = None
    while query_response is None:
        try:
            query_response = requests.request("GET", query_url, headers=headers).json()
            if '_error' in query_response:
                time.sleep(10)
                query_response = None
        except Exception as error:
            print(error)
            pass
    return query_response


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
        """
        :param start_date: have to be datetime
        :param day_range: how many days you want to check
        :param tag:
        :param inbox:
        :return: list of conversation in a given day by default
        """
        dates = _get_date_range(start_date, day_range)
        query_url = _create_url(dates[0], dates[1], tag, inbox)
        searched_data = {}
        first_page = True
        while query_url is not None:
            query_response = _query_to_api(query_url)
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
