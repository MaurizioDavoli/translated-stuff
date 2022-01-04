"""tool to get parsed data from front"""
from datetime import datetime
import datetime
from pytz import timezone
import logging

from services.front_api_utility import FrontApiUtility


# offers_tag = ['automatic',  'forwarded',  'dragged_ui', 'maybe_offer']
# OFFERS_TAG = ['tag_1dxwxy', 'tag_1g44qu', 'tag_1j5rie', 'tag_1j5rra']
OFFERS_TAG = ['tag_1g44qu', 'tag_1j5rie']


def _from_timestampms_to_datetime_cet(timestamp_ms):
    """time zone handling utility"""
    utc_dt = datetime.datetime.fromtimestamp(timestamp_ms).replace(microsecond=0)
    arrival_date = utc_dt.astimezone(timezone('Europe/Rome')).replace(tzinfo=None)
    return arrival_date


def _get_contact_mail(row, target):
    for elem in row['last_message']['recipients']:
        if elem['role'] == target:
            return elem['handle']


class FrontUtility:

    api_utility = None

    def __init__(self, token):
        self.api_utility = FrontApiUtility(token)

    def get_parsed_last_week_conversations(self, tag=None, inbox=None):
        """:return all the conversation in a friendly format"""
        parsed_list = []
        logging.info("still working.. it may take a bit")
        conversations = self.api_utility.get_last_week_conversations(tag=tag, inbox=inbox)
        if conversations:
            for row in conversations:
                parsed_list.append((row['id'],
                                    _get_contact_mail(row, 'from'),
                                    _get_contact_mail(row, 'from'),
                                    row['subject'][:450],
                                    _from_timestampms_to_datetime_cet(row['created_at']),
                                    _get_contact_mail(row, 'to'),
                                    row['last_message']['body'][:3500]
                                    ))
        return parsed_list

    def get_tagged_parsed_last_week_conversations(self):
        """:return a list of conversation in a friendly format that are tagged with the content of OFFERS_TAG"""
        parsed_list = []
        for tag in OFFERS_TAG:
            conversations = self.get_parsed_last_week_conversations(tag=tag)
            for conv in conversations:
                tag_list = [tag]
                nice_list = [*tag_list, *conv]
                parsed_list.append(nice_list)
        return parsed_list
