import pandas as pd
import json
import glob
import os


# function to get column names
def get_column_names(schemas, schemaName, sortKey=None):
    column_list = list(
        map(lambda order: order['column_name'], list(schemas.get(schemaName))))
    # return column_list
    if (sortKey != None):
        sorted_list = sorted(list(schemas.get(schemaName)),
                             key=lambda order: order.get(sortKey))
        return list(map(lambda order: order['column_name'], list(sorted_list)))
    return column_list


# returns list of recursive csv file path
# path = 'data/retail_db/*/*'
def get_input_file_list(path):
    return glob.glob(path, recursive=True)


# path f'data/retail_db_json/{collection_name}'
def ensureDirectoryExists(path):
    os.makedirs(path, exist_ok=True)


def read_csv(filePath, columns):
    return pd.read_csv(filePath, names=columns)


def get_schema():
    return json.load(open('./data/retail_db/schemas.json'))


def process_files(filesList):
    for file_name in filesList:
        # capture collection name from file path
        collection_name = file_name.split('/')[-2]

        # enure directory exists for output
        ensureDirectoryExists(f'data/retail_db_json/{collection_name}')

        print(f'Processing file : {file_name}')

        # csv headers
        column_headers = get_column_names(get_schema(), collection_name)

        # read csv
        collection = read_csv(file_name, column_headers)

        # create json file for csv collection

        collection.to_json(f'data/retail_db_json/{collection_name}/part-00000')


if __name__ == '__main__':
    path = 'data/retail_db/*/*'
    fileLists = get_input_file_list(path)
    process_files(fileLists)
