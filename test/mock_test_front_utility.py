from unittest.mock import Mock


def _query_to_api():
    """:return json result to get query at a given url"""
    query_response = None
    attempt = 0
    while query_response is None and attempt < 10:
        try:

            query_response = Mock()
            query_response.return_value(404)

            if '_error' in query_response:
                print(query_response)
                attempt = attempt + 1
                query_response = None
        except Exception as error:
            print(error)
            attempt = attempt+1
    return query_response


_query_to_api()