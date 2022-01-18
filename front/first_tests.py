"""testing some get request to front api"""
import requests

import pandas as pd

import matplotlib.pyplot as plt

import os


access_token = os.environ['FRONT_TOKEN']

url = "https://api2.frontapp.com/"

FRONT_API_REQUEST_HEADER = {
    "Accept": "application/json",
    "Authorization": "Bearer " + access_token
}


def print_uncropped_data_frama(data_frame):
    """printing method to avoid code duplication"""
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(data_frame)


def get_x_number_of_conversation(x):
    """return .json file with a list of conversation"""
    url_conv = url+'conversations?limit='+str(x)
    response = requests.request("GET", url_conv, headers=FRONT_API_REQUEST_HEADER)
    # pretty_json = json.loads(response.text)
    # print(json.dumps(pretty_json, indent=2))
    data_frame = pd.DataFrame(response.json()['_results'])
    cropped_df = data_frame.drop(["_links",
                                  "recipient",
                                  "assignee",
                                  "tags",
                                  "links",
                                  "topics",
                                  "last_message",
                                  "is_private",
                                  "scheduled_reminders",
                                  "metadata"], axis=1)
    return data_frame


def get_conversation_data(id_conv):
    url_conv = url+"conversations/"+str(id_conv)
    response = requests.request("GET", url_conv, headers=FRONT_API_REQUEST_HEADER)
    data_frame = pd.DataFrame(response.json()["tags"]).drop(["_links",
                                                             "highlight",
                                                             "is_visible_in_conversation_lists",
                                                             "is_private"], axis=1)
    id_set = response.json()["id"]
    object_set = response.json()["subject"]
    data_frame["conversation_id"] = id_set
    data_frame["subject"] = object_set
    return data_frame


def get_more_conversation_data(x):
    data_frame_conv = get_x_number_of_conversation(x)
    data_frame = pd.DataFrame()
    for row in data_frame_conv.iterrows():
        data = get_conversation_data(row[1]["id"])
        data_frame = data_frame.append(data)
    return data_frame


def get_plot_tag(x):
    base_data_set = get_more_conversation_data(x)
    print_uncropped_data_frama(base_data_set)
    dict_set = {}
    for row in base_data_set.iterrows():
        element = row[1]["name"]
        if element in dict_set:
            dict_set[element] = dict_set[element]+1
        else:
            dict_set[element] = 1
    serie = pd.Series(dict_set)
    serie.plot.bar(title=str(x)+' conversazioni controllate')
    plt.tight_layout()
    plt.show()
    plt.close('all')


get_plot_tag(30)