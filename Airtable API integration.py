import json
import os

import gdown
import pandas as pd
import requests

# # ------------------------ Google Drive Section ------------------------------
# Download file from google drive
file_url = ''
file_id = file_url.split('/')[-2]
prefix = 'https://drive.google.com/uc?/export=download&id='
gdown.download(prefix + file_id)

# Enter the path of csv file downloaded from google drive, do not use ".csv" after file name
csv_file_path = ''

# -------------------- Airtable Section -----------------------

INPUT_TABLE_AIRTABLE_COLUMN_NAMES = []
INPUT_TABLE_EQUIVALENT_GSHEET_COLUMN_NAMES = []

OUTPUT_AIRTABLE_COLUMN_NAMES = []
OUTPUT_TABLE_EQUIVALENT_GSHEET_COLUMN_NAMES = []

# AirTable credentials
AIRTABLE_API_KEY = ""

AIRTABLE_INPUT_BASE_ID = ""
AIRTABLE_INPUT_TABLE_ID = ""
AIRTABLE_INPUT_VIEW_ID = ""

AIRTABLE_OUTPUT_BASE_ID = ""
AIRTABLE_OUTPUT_TABLE_ID = ""
AIRTABLE_OUTPUT_VIEW_ID = ""

headers = {
    'Authorization': f'Bearer {AIRTABLE_API_KEY}',
    'Content-Type': 'application/json'
}

params = {
    "view": AIRTABLE_INPUT_VIEW_ID
}

output_params = {
    "view": AIRTABLE_OUTPUT_VIEW_ID
}



all_records = []

def search_and_match_records(data, base_id, table_id, view_id, search_fields, remaining_data_to_post):
    '''
    This function checks in airtable records if the record is already exists (matching criteria: 'column_name_1', 'column_name2') and if not, it creates new records
    '''
    for record in data:
        match_count = 0
        for field, value in search_fields.items():
            airtable_record_value = record['fields'][field].strip()
            excel_record_to_match = value.strip()

            if field == '' and is_record_value(airtable_record_value, excel_record_to_match):
                match_count += 1
            elif field == '' and is_record_value(airtable_record_value, excel_record_to_match):
                match_count += 1

            if match_count == 2:
                record_id = record['id']
                return make_resulting_dict_object(remaining_data_to_post, "True")

    create_new_record(base_id, table_id, view_id, remaining_data_to_post)
    return make_resulting_dict_object(remaining_data_to_post, "False")


def make_resulting_dict_object(remaining_data_to_post, match_value):
    '''
    creates dictionary of data
    '''
    return {
        **remaining_data_to_post,
        'MATCH': match_value}


def create_new_record(base_id, table_id, view_id, fields):
    '''
    Creates new reord in AirTable
    '''
    data = {
        'fields': {
            **fields,
        }
    }
    url = f'https://api.airtable.com/v0/{base_id}/{table_id}'
    response = requests.post(
        f'{url}?view={view_id}', headers=headers, data=json.dumps(data))


def is_record_value(record_value, search_value):
    '''
    Matches data from AirTable and csv file and returns True if data matches and vice versa
    '''
    parsed_record_code = record_value
    parsed_search_code = search_value

    if parsed_record_code == parsed_search_code:
        return True
    return False


def loop_all_records(data, base_id, table_id, view_id, selected_columns_df):
    all_records_to_add_in_output_csv = []
    for index, row in selected_columns_df.iterrows():
        record_dict = row.to_dict()
        record_dict[OUTPUT_AIRTABLE_COLUMN_NAMES[0]] = str(
            str(record_dict[OUTPUT_AIRTABLE_COLUMN_NAMES[0]]))
        records_to_match = {k: record_dict[k]
                            for k in OUTPUT_AIRTABLE_COLUMN_NAMES}
        data_to_post = record_dict.copy()
        record_to_add_in_output_csv = search_and_match_records(
            data, base_id, table_id, view_id, records_to_match, data_to_post)
        all_records_to_add_in_output_csv.append(record_to_add_in_output_csv)
    return all_records_to_add_in_output_csv


def output_results_to_excel(all_records_to_add_in_output_csv):
    '''
    creates new output.xlsx file if it does not exist, and if it exists it appends data to that file
    Updates data in output file with boolean value
    '''
    output_file_path = os.path.join(os.getcwd(), 'output.xlsx')

    if os.path.exists(output_file_path):
        existing_file = pd.read_excel('output.xlsx')
        output_excel_dataframe = pd.DataFrame(all_records_to_add_in_output_csv)
        df_concatenated = pd.concat(
            [existing_file, output_excel_dataframe], axis=0)
        df_concatenated.to_excel('output.xlsx', index=False)
    else:
        output_excel_dataframe = pd.DataFrame(all_records_to_add_in_output_csv)
        output_excel_dataframe.to_excel('output.xlsx', index=False)


def main():
    # Url of prospects table
    url = f'https://api.airtable.com/v0/{AIRTABLE_INPUT_BASE_ID}/{AIRTABLE_INPUT_TABLE_ID}'

    response = requests.get(f'{url}', headers=headers, params=params)
    data = json.loads(response.text)
    all_records.extend(data['records'])

    # If there is more than 100 records below function will run
    while True:
        url = url
        if 'offset' in data:
            offset_url = f'{url}?offset={data["offset"]}'

            response = requests.get(offset_url, headers=headers, params=params)
            data = json.loads(response.text)
            all_records.extend(data['records'])
        else:
            break

    all_data = []
    for records in all_records:
        row = {'id': records['id']}
        row.update(records['fields'])
        all_data.append(row)

    
    
    f5500_data = []
    
    # Url of output table
    f5500_url = f'https://api.airtable.com/v0/{AIRTABLE_OUTPUT_BASE_ID}/{AIRTABLE_OUTPUT_TABLE_ID}'

    output_response = requests.get(
        f'{f5500_url}', headers=headers, params=output_params)
    output_data = json.loads(output_response.text)
    f5500_data.extend(output_data['records'])

    # If there is more than 100 records, below function will run
    while True:
        url = f5500_url
        if 'offset' in output_data:
            f5500_offset_url = f'{url}?offset={output_data["offset"]}'

            output_response = requests.get(
                f5500_offset_url, headers=headers, params=output_params)
            output_data = json.loads(output_response.text)
            f5500_data.extend(output_data['records'])
        else:
            break
    
    # Create dataframe using data from prospects table
    prospects_df = pd.DataFrame(all_data)
    prospects_df = prospects_df[INPUT_TABLE_AIRTABLE_COLUMN_NAMES]
    prospects_df = prospects_df.applymap(
        lambda s: s.lower() if type(s) == str else s)
    prospects_df.replace('[,.]', '', regex=True, inplace=True)

    # Creates dataframe using csv file downloaded from google drive
    gsheet_df = pd.read_csv(f'{csv_file_path}.csv', low_memory=False)
    gsheet_df = gsheet_df[INPUT_TABLE_EQUIVALENT_GSHEET_COLUMN_NAMES]
    gsheet_df = gsheet_df.applymap(
        lambda s: s.lower() if type(s) == str else s)
    gsheet_df.replace('[,.]', '', regex=True, inplace=True)

    
    # Merges data from prospects_dfd ans gsheet_df if "Entity" and "HQ" from prospects_df and 'SF_SPONSOR_NAME' and''SF_SPONS_US_CITY' from gsheet_df matches
    merged_data = pd.merge(prospects_df, gsheet_df, left_on=['column_name_1', 'column_name_2'], right_on=['column_name_1', 'column_name_2'])
    
    # Removes duplicate data from merges_df
    merged_data = merged_data[~merged_data[''].duplicated(keep='first')]
    merged_data = merged_data.fillna('').astype(str)
    merged_data = merged_data.applymap(
        lambda s: s.title() if type(s) == str else s)

    all_records_to_add_in_output_csv = loop_all_records(
        f5500_data, AIRTABLE_OUTPUT_BASE_ID, AIRTABLE_OUTPUT_TABLE_ID, AIRTABLE_OUTPUT_VIEW_ID, merged_data)
    output_results_to_excel(all_records_to_add_in_output_csv)


if __name__ == '__main__':
    main()
