"""tool to get simple operation with the staging db"""

from services.mysql_db_dao import MySqlDbDao


class MySqlDbUtility:

    db_worker = None

    def __init__(self, connection):
        self.db_worker = MySqlDbDao(connection)

    def get_parsed_offer(self, creation_date, sender_mail):
        """:return offer info in a friendly format"""
        offers = self.db_worker.get_offer(creation_date, sender_mail)
        parsed_offer = []
        if offers and len(offers) == 1:
            # remove not used attributes
            offer = list(offers[0][:29])
            offer.pop(17)
            offer.pop(26)
            parsed_offer.append(tuple(offer))

        return parsed_offer

