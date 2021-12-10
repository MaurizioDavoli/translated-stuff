
def merge_db_front(db_obj, front_obj):

    amazing_list = []
    for elem in front_obj:
        amazing_list.append(elem)
    for elem in db_obj:
        amazing_list.append(elem)

    return amazing_list
