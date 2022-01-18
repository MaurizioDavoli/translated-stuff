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
            if validate_elem(to_append):
                to_append.pop(0)
                if to_append not in merged_list:
                    merged_list.append(to_append)
        else:
            front_obj.pop(0)
            if validate_elem(front_obj) and front_obj not in merged_list:
                merged_list.append(front_obj)
    return merged_list


def validate_elem(processed_elem):
    """logic for validate good data for training"""
    if not processed_elem:
        return False
    if len(processed_elem) < 10:
        return True
    tag = processed_elem[0]
    status = processed_elem[27]
    if ('tag_1dxwxy' == tag or 'tag_1g44qu' == tag) and status in POSITIVE_STATUS:      # automatic
        return True
    return False
