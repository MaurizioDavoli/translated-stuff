from services.mysql_utility import MySqlDbUtility
import os

POSITIVE_STATUS = ['accepted', 'sent', 'refused']


def _merge_db_front(db_obj, front_obj):
    amazing_list = []
    for elem_front in front_obj:
        amazing_list.append(elem_front)
    for elem_db in db_obj[0]:
        amazing_list.append(elem_db)
    return amazing_list


def merge_db_front(to_check_conversations):
    mysql_tool = MySqlDbUtility(os.environ['MYSQL_DB_USER'],
                                os.environ['MYSQL_DB_PSWD'],
                                os.environ['MYSQL_DB_HOST'],
                                os.environ['MYSQL_DB_NAME'])
    merged_list = []
    for front_obj in to_check_conversations:
        db_obj = mysql_tool.get_parsed_offer(front_obj[4], front_obj[5][0][1])
        if db_obj:
            merged_list.append(_merge_db_front(db_obj, front_obj))
    return merged_list


def validate_elem(processed_elem):
    if not processed_elem:
        return False
    tags = processed_elem[3]
    status = processed_elem[14]
    if 'automatic' in tags:
        print("auto" + str(tags) + str(status))
        if status in POSITIVE_STATUS:
            return True
    if 'dragged-ui' in tags:
        print("drag" + str(tags) + str(status))
        if status in POSITIVE_STATUS:
            return True
    return False


