import os
from datetime import datetime, timedelta
import urllib.parse

import requests

ACCESS_TOKEN = None

url = "https://api2.frontapp.com/"

headers = {}


def _get_date_range(start_date, date_range):
    """:return a touple of start and end date"""
    init_date = datetime.timestamp(start_date)
    end_date = datetime.timestamp(start_date + timedelta(days=date_range))
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
        :param start_date:
        :param token:
        :param day_range:
        :param tag:
        :param inbox:
        :return:
        """
        dates = _get_date_range(start_date, day_range)
        query_url = _create_url(dates[0], dates[1], tag, inbox)
        response = None
        while response is None:
            try:
                response = requests.request("GET", query_url, headers=headers)
            except Exception:
                pass
        return response.json()

