from dags.services.front_utility import FrontUtility
from dags.services.merge_utility import merge_db_front


def test_write():
    front_tool = FrontUtility("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzY29wZXMiOlsic2hhcmVkOioiLCJwcml2YXRlOioiXSwiaWF0IjoxNjM4NDQ5OTY0LCJpc3MiOiJmcm9udCIsInN1YiI6IjU1MDcxODJjNDc0ZGY3Njc4YzU4IiwianRpIjoiNGVmZTE5OTcwYTA5OTQxNyJ9.l6Y8T1kYUGTdmeoCfKgcRTsWYBpvWdCTTQA4-Iw0trU")
    to_check_conversations = front_tool.get_tagged_yesterday_parsed_conversations()
    merged_list = merge_db_front(to_check_conversations)
    for elem in merged_list:
        print(elem)


test_write()
