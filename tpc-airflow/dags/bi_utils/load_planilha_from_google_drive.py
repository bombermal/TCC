# function:
#  download_from_google_drive
#  parameters: drive authentication, file id, file name

import io
import pandas as pd
from apiclient.http import MediaIoBaseDownload 
from datetime import datetime, timedelta


import base64


from bi_utils.postgres_star_schema import create_table_from_dataframe_postgresql, populate_table_from_dataframe_postgresql

def download_file_google_drive(
        file_id,
        mime_type='text/csv',
        taks_authenticate_google_id='task_authenticate_google_api',
        save_filepath = '/opt/airflow/extract/file.csv',
        **kwargs
    ):
    
    # Puxa o serviço autenticado do Google Drive
    # da task anterior 
    ti = kwargs['ti']
    drive_service = ti.xcom_pull(task_ids=taks_authenticate_google_id, key='return_value')
    request = drive_service.files().export_media(fileId=file_id,mimeType=mime_type)


    # download the file to a BytesIO object
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))
    fh.seek(0)

    # save dataframe
    df = pd.read_csv(fh)
    df.to_csv(save_filepath, index=False)


def handle_carriage_return_on_dataframes(
    df
):
    """
    Remove carriage return characters from dataframes to avoid errors when loading to postgres

    Parameters
    ----------
        df: dataframe 
            Dataframe to be handled

    Returns
    -------
        dataframe
            with carriage return removed
    """
    print('Handling carriage return on dataframes')

    # Remove carriage return
    df = df.replace('\r', '"\r"', regex=True)

    return df


def download_file_google_drive_and_save_to_postgresql(
        file_id,
        mime_type='text/csv',
        taks_authenticate_google_id='task_authenticate_google_api',
        schema_name='public',
        table_name='table_name',
        postgres_conn_id='postgres_default',
        drop_previous_records=True,
        delimiter=None,
        header='infer',
        encoding=None,
        **kwargs
    ):
    
    # Puxa o serviço autenticado do Google Drive
    # da task anterior 
    ti = kwargs['ti']
    drive_service = ti.xcom_pull(task_ids=taks_authenticate_google_id, key='return_value')
    request = drive_service.files().export_media(fileId=file_id,mimeType=mime_type)


    # download the file to a BytesIO object
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))
    fh.seek(0)

    # save dataframe
    df = pd.read_csv(fh, delimiter=delimiter, header=header, encoding=encoding)

    # transformations
    df = handle_carriage_return_on_dataframes(df)

    # save dataframe in postgres
    create_table_from_dataframe_postgresql(
        df,
        schema_name=schema_name,
        table_name=table_name,
        postgres_conn_id=postgres_conn_id,
        replace_table=drop_previous_records
    )

    populate_table_from_dataframe_postgresql(
        df,
        schema_name=schema_name,
        table_name=table_name,
        postgres_conn_id=postgres_conn_id,
        drop_previous_records=drop_previous_records
    )

def download_file_gmail_and_save_to_postgresql(
        subject,
        taks_authenticate_gmail_id='task_authenticate_gmail_api',
        schema_name='public',
        table_name='table_name',
        postgres_conn_id='postgres_default',
        drop_previous_records=True,
        delimiter=None,
        header='infer',
        encoding=None,
        **kwargs
    ):

    # Puxa o serviço autenticado do Google Drive
    # da task anterior 
    ti = kwargs['ti']
    gmail_service = ti.xcom_pull(task_ids=taks_authenticate_gmail_id, key='return_value')

    # Pesquisa por emails com o título especificado
    query = f'subject: {subject}'

    # Call the Gmail API
    results = gmail_service.users().messages().list(userId='me', q=query).execute()
    #print(results)
    messages = results.get('messages', [])

    # Pega o ID do email mais recente
    earliest_email_id = messages[0]['id']
  
    # Armazena o conteúdo do email
    email_content = gmail_service.users().messages().get(userId='me', id=earliest_email_id).execute()

    # Pega do conteúdo do email a informação da data de envio do email em formato datetime
    email_date = datetime.fromtimestamp(int(email_content['internalDate'])/1000)
    # Atualiza a data com o fuso horário do Brasil (UTC-3)
    email_date = email_date - timedelta(hours=3)
    print(email_date)

    # Pega o ID do anexo
    attachment_id = email_content['payload']['parts'][1]['body']['attachmentId']

    # Pega o conteúdo do anexo
    attachment = gmail_service.users().messages().attachments().get(userId='me', messageId=earliest_email_id, id=attachment_id).execute()

    # Decodifica o conteúdo do anexo
    file_data = base64.urlsafe_b64decode(attachment['data'].encode(encoding))
    #print(file_data)
    # Salva o conteúdo do anexo em um dataframe
    df = pd.read_csv(io.StringIO(file_data.decode('utf-8')), delimiter=delimiter, header=header, encoding=encoding)

    # Insere a data de envio do email como primeira coluna no dataframe
    df.insert(0, 'data_email', email_date)


    # transformations
    df = handle_carriage_return_on_dataframes(df)

    # save dataframe in postgres
    create_table_from_dataframe_postgresql(
        df,
        schema_name=schema_name,
        table_name=table_name,
        postgres_conn_id=postgres_conn_id,
        replace_table=drop_previous_records
    )

    populate_table_from_dataframe_postgresql(
        df,
        schema_name=schema_name,
        table_name=table_name,
        postgres_conn_id=postgres_conn_id,
        drop_previous_records=drop_previous_records
)