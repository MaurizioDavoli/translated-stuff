"""tool to get parsed data from front"""
from datetime import datetime
import datetime
from pytz import timezone
import logging

from services.front_api_utility import FrontApiUtility


# offers_tag = ['automatic',  'forwarded',  'dragged_ui', 'maybe_offer']
# OFFERS_TAG = ['tag_1dxwxy', 'tag_1g44qu', 'tag_1j5rie', 'tag_1j5rra']
OFFERS_TAG = ['tag_1dxwxy']


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

    api_utility = None

    def __init__(self, token):
        self.api_utility = FrontApiUtility(token)

    def get_parsed_last_week_conversations(self, tag=None, inbox=None):
        """:return all the conversation in a friendly format"""
        conversations = self.api_utility.get_last_week_conversations(tag=tag, inbox=inbox)
        parsed_list = []
        logging.info("still working.. it may take a bit")
        for row in conversations:
            parsed_list.append((_parse_contact(self.api_utility.
                                               get_contact_handle(row['recipient']['_links']['related']['contact'])),
                                row['subject'][:450],
                                _from_timestampms_to_datetime_cet(row['created_at']),
                                self.api_utility.get_body(row['_links']['related']['last_message'])[:3500],
                                _parse_tag(row['tags']),
                                row['id']
                                ))
        return parsed_list

    def get_tagged_parsed_last_week_conversations(self):
        """:return a list of conversation in a friendly format that are tagged with the content of OFFERS_TAG"""
        parsed_list = []
        for tag in OFFERS_TAG:
            conversations = self.get_parsed_last_week_conversations(tag=tag)
            for conv in conversations:
                parsed_list.append(conv)
        return parsed_list
