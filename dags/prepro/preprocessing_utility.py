import pandas as pd

from preprocessing.datasets.data_preprocessing import \
    improve_data_emails, \
    clean_email_dataset, \
    get_years, \
    create_quote_labels, \
    make_email_and_offer_data, \
    make_quote_classification_data, \
    decode_text_from_classification_data, \
    cut_too_small_text \

COLUMNS_NAMES = ['front_id', 'sender', 'from_sender', 'subject', 'date', 'receiver', 'body', 'id_offer',
                 'important', 'notarization', ' id_project', 'id_auto_request', ' id_customer', 'customer_type',
                 'requester_email', 'requester_name', 'requester_tel', 'lang', 'am', 'arrival_datetime',
                 'am_fetch_datetime', 'response_date', 'target_response_date', 'sys_response_date',
                 'source_channel', 'fax_confirm_request', 'status', 'accept_refuse_datetime', 'fax_confirm_date',
                 'first_offer', 'current_offer', 'accepted_offer', 'assigned_to_pm', 'suggested_translator',
                 'message-id']


def preproces_last_loaded(last_loaded):

    # create dataframe from list of touple
    df = pd.DataFrame(last_loaded, columns=COLUMNS_NAMES).reset_index()

    # preprocessing operations
    email_info_data = df.filter(items=['message-id', 'subject', 'date', 'receiver', 'body'])\
        .pipe(improve_data_emails)\
        .pipe(clean_email_dataset)\
        .pipe(get_years)
    email_offer_data = df.filter(items=['message-id', 'id_project', 'id_customer', 'lang', 'status', 'body'])\
        .pipe(create_quote_labels)
    email_offer_data = make_email_and_offer_data(email_info_data, email_offer_data)
    email_offer_data = email_offer_data.pipe(make_quote_classification_data).\
        pipe(decode_text_from_classification_data).\
        pipe(cut_too_small_text, 3)

    # cast to list for writing on db
    prepocessed_list = email_offer_data.to_numpy().tolist()

    return prepocessed_list

