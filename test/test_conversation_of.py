from datetime import datetime

from front.get_message_from_date import get_conversations_of


def test_date_accuracy():
    date = datetime(2021, 10, 1)
    loaded_data_frame = get_conversations_of(date)
    print(loaded_data_frame)


test_date_accuracy()
