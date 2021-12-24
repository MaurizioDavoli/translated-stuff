"""tool to get simple the operations with the front api"""
import time
from datetime import datetime, timedelta
import datetime
from pytz import timezone
import logging

import urllib.parse
import requests


FRONT_API_SEARCH_PATH = "https://api2.frontapp.com/conversations/search/"


# offers_tag = ['automatic',  'forwarded',  'dragged_ui', 'maybe_offer']
# OFFERS_TAG = ['tag_1dxwxy', 'tag_1g44qu', 'tag_1j5rie', 'tag_1j5rra']
OFFERS_TAG = ['tag_1dxwxy']


def _get_date_range(start_date, date_range):
    """:return a touple of start and end date"""
    init_date = datetime.datetime.timestamp(start_date)
    end_date = datetime.datetime.timestamp(start_date + timedelta(days=date_range))
    return init_date, end_date


def _create_url(start, end, tag, inbox):
    """:return encoded url for get request"""
    query_dict = {'after': ' after:'+str(start),
                  'before': ' before:'+str(end)
                  }
    if tag is not None:
        query_dict['tag'] = ' tag:'+tag
    if inbox is not None:
        query_dict['inbox'] = ' inbox:'+inbox
    query_to_encode = ''.join(query_dict.values())
    return FRONT_API_SEARCH_PATH + urllib.parse.quote(query_to_encode)


def _parse_tag(tag_list):
    """query to front tag information and return these parsed"""
    parsed_list = []
    for row in tag_list:
        parsed_list.append(row['name'])
    return parsed_list


def _parse_contact(contact_list):
    """query contact info to front api and return source and info"""
    parsed_list = []
    for row in contact_list:
        parsed_list.append((row['source'],
                            row['handle']))
    return parsed_list


def _from_timestampms_to_datetime_cet(timestamp_ms):
    """time zone handling utility"""
    utc_dt = datetime.datetime.fromtimestamp(timestamp_ms).replace(microsecond=0)
    arrival_date = utc_dt.astimezone(timezone('Europe/Rome')).replace(tzinfo=None)
    return arrival_date


class FrontUtility:

    access_token = None
    front_api_request_headers = {}

    def __init__(self, token):
        self.access_token = token
        self.front_api_request_headers = {
            "Accept": "application/json",
            "Authorization": "Bearer " + self.access_token
        }

    def query_to_api(self, query_url):
        """:return json result to get query at a given url"""
        query_response = None
        while query_response is None:
            try:
                query_response = requests.request("GET", query_url, headers=self.front_api_request_headers).json()
                if '_error' in query_response:
                    logging.error(query_response)
                    time.sleep(1)
                    query_response = None
            except Exception as error:
                query_response = None
                logging.error(error)
        return query_response

    def get_conversations(self, start_date, day_range=1, tag=None, inbox=None):
        """:return a list of conversation with a date as input"""
        dates = _get_date_range(start_date, day_range)
        query_url = _create_url(dates[0], dates[1], tag, inbox)
        searched_data = {}
        first_page = True
        while query_url is not None:
            query_response = self.query_to_api(query_url)
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

    def get_last_week_conversations(self, tag=None):
        """:return the conversation from last_week"""
        last_week = datetime.datetime.now() - timedelta(days=7)
        last_week_ok = datetime.datetime(last_week.year, last_week.month, last_week.day)
        to_check_conversations = {}
        try:
            response = self.get_conversations(last_week_ok, tag=tag)
            to_check_conversations = response['_results']
            logging.info(response['_total'])
        except RecursionError:
            logging.error("recursion error")
        return to_check_conversations

    def get_contact(self, contact_url):
        """:return a contact descriprion"""
        response = self.query_to_api(contact_url)['handles']
        if response is None:
            return []
        return response

    def get_body(self, message_link):
        return self.query_to_api(message_link)['body']

    def get_parsed_last_week_conversation(self, tag=None):
        """:return all the conversation in a friendly format"""
        conversations = self.get_last_week_conversations(tag=tag)
        parsed_list = []
        logging.info("still working.. it may take a bit")
        for row in conversations:
            parsed_list.append((_parse_contact(self.get_contact(row['recipient']['_links']['related']['contact'])),
                                row['subject'][:450],
                                _from_timestampms_to_datetime_cet(row['created_at']),
                                self.get_body(row['_links']['related']['last_message'])[:3500],
                                _parse_tag(row['tags']),
                                row['id']
                                ))
        return parsed_list

    def get_tagged_last_week_parsed_conversations(self):
        """:return a list of conversation in a friendly format that are tagged with the content of OFFERS_TAG"""
        parsed_list = []
        for tag in OFFERS_TAG:
            conversations = self.get_parsed_last_week_conversation(tag=tag)
            for conv in conversations:
                parsed_list.append(conv)
        return parsed_list
