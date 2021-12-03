from datetime import datetime

from front import get_message_from_date as to_test

i

def test_date_accuracy():
    date = datetime(2021, 10, 1)
    loaded_data_frame = to_test.get_conversations_of(date)
    print(loaded_data_frame)


test_date_accuracy()
