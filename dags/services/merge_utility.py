# status that are good for training
POSITIVE_STATUS = ['accepted', 'sent', 'refused']


def _merge_db_front(db_obj, front_obj):
    """:return a merged list of db_obj and front_obj"""
    amazing_list = []
    for elem_front in front_obj:
        amazing_list.append(elem_front)
    for elem_db in db_obj[0]:
        amazing_list.append(elem_db)
    return amazing_list


def merge_db_front(to_check_conversations, mysql_tool):
    """:return if exist a merged list of front data and staging db
        pairing based on sender and arrival_datetime"""
    merged_list = []
    for front_obj in to_check_conversations:
        db_obj = mysql_tool.get_parsed_offer(front_obj[5], front_obj[2])
        if db_obj:
            to_append = _merge_db_front(db_obj, front_obj)
            if validate_elem(to_append) and to_append.pop(0) not in merged_list:
                print(to_append)
                merged_list.append(to_append)
        elif validate_elem(front_obj):  # and front_obj.pop(0) not in merged_list:
            print(front_obj.pop(0) not in merged_list)
            merged_list.append(front_obj)
    return merged_list


def validate_elem(processed_elem):
    """logic for validate good data for training"""
    if not processed_elem:
        return False
    if len(processed_elem) < 10:
        return True
    tags = processed_elem[0]
    status = processed_elem[27]
    if ('tag_1dxwxy' in tags or 'tag_1g44qu' in tags) and status in POSITIVE_STATUS:      # automatic
        return True
    return False
