"""class that offer the capability of looking for conversation in a given date"""
import os
import urllib.parse
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import pandas as pd
import requests

from services.front_utility import FrontUtility

ACCESS_TOKEN = os.environ['FRONT_TOKEN']

url = "https://api2.frontapp.com/"

headers = {
    "Accept": "application/json",
    "Authorization": "Bearer "+ACCESS_TOKEN
}


def get_date_range(start_date, date_range):
    """return a touple of start and end date"""
    init_date = datetime.timestamp(start_date)
    end_date = datetime.timestamp(start_date + timedelta(days=date_range))
    print(start_date)
    print(datetime.fromtimestamp(end_date))
    print(init_date)
    print(end_date)
    return init_date, end_date


def create_url(date_tuple, tag=None):
    """create the url for the http request"""
    tag_query = ""
    if tag is not None:
        tag_query = " tag:"+tag
    date_query = "after:"+str(date_tuple[0])+" before:"+str(date_tuple[1])
    query = date_query+tag_query
    query_url = urllib.parse.quote(query)
    final_url = url+"conversations/search/"+query_url
    return final_url


def print_uncropped_data_frama(data_frame):
    """printing method to avoid code duplication"""
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(data_frame)


def get_conversations_of(start_date, time_zone, day_range=1,  tag=None):
    """
    :param start_date:
    :param time_zone:
    :param day_range: number of days you want to search from the start_date
    :param tag: tag of the conversation you want to look for
    :return: pandas dataframe with id and date of what you search
    """
    dates = get_date_range(start_date, day_range)
    query_url = create_url(dates, tag)
    print(query_url)
    data_frame = None
    quantity = 0
    while data_frame is None:
        try:
            response = requests.request("GET", query_url, headers=headers)
            print(response.json()["_total"])
            quantity = response.json()["_total"]
            data_frame = pd.DataFrame(response.json()["_results"]).drop(["_links",
                                                                         "status",
                                                                         "assignee",
                                                                         "recipient",
                                                                         "links",
                                                                         "tags",
                                                                         "topics",
                                                                         "last_message",
                                                                         "is_private",
                                                                         "scheduled_reminders",
                                                                         "metadata"], axis=1)
        except:
            pass

    return data_frame, quantity


def plot_maybe_offer_october():
    day = 1
    dict_set = {}
    while day < 31:
        data = datetime(2021, 11, day)
        maybe_offer = get_conversations_of(data, "UTC", tag="tag_1j5rra")[1]
        print(maybe_offer)
        dict_set[str(data).removesuffix("00:00:00")] = maybe_offer
        day = day+1
    serie = pd.Series(dict_set)
    serie.plot.bar(title='MAYBY OFFER IN NOVEMBRE')
    plt.tight_layout()
    plt.show()
    plt.close('all')

    print(dict)


#plot_maybe_offer_october()

tool = FrontUtility(ACCESS_TOKEN)
tool.get_conversations(datetime(2021, 11, 1), tag="tag_1j5rra")


# tag_1j5rra = maybe offer
