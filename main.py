import csv
import os
from multiprocessing import Pool


def map_reduce(clicks_file, users_file, chunks_num, chunks_num2):
    # 1st step - reading the csv files using "csv" module
    with open(clicks_file, 'r') as csvfile:
        reader_csv = csv.DictReader(csvfile)
        clicks_dataset = []
        for row in reader_csv:
            clicks_dataset.append(row)
    with open(users_file, 'r') as csvfile2:
        reader_csv2 = csv.DictReader(csvfile2)
        users_dataset = []
        for row in reader_csv2:
            users_dataset.append(row)

    # 2nd step - splitting data into chunks (both input files)
    def split_dataset(dataset, num_chunks):
        rows_total = len(dataset)
        rows_remain = rows_total % num_chunks
        chunk_size = (rows_total - rows_remain) // num_chunks + (rows_remain > 0)
        return [dataset[i:i + chunk_size] for i in range(0, len(dataset), chunk_size)]

    chunks_clicks = split_dataset(clicks_dataset, chunks_num)
    chunks_users = split_dataset(users_dataset, chunks_num2)

    # 3rd step - using "multiprocessing" module to process mapped data in parallel
    with Pool() as pool:
        mapped_users_list = pool.map(mapping_users, chunks_users)
        mapped_users_dict = {k: v for user_dict in mapped_users_list for k, v in user_dict.items()}
        params = [(mapped_users_dict, chunk) for chunk in chunks_clicks]
        mapped_clicks_chunks = pool.starmap(mapping_clicks, params)

    # 4th step - creating final CSV file with filtered/mapped data
    output_file = 'final.csv'
    output_dir_path = 'data/filtered_clicks'
    output_file_path = os.path.join(output_dir_path, output_file)
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    with open(output_file_path, 'w', newline='') as csvfile:
        fieldnames = ['date', 'user_id', 'click_target']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for chunk in mapped_clicks_chunks:
            writer.writerows(chunk)


# Functions used for 3rd step:
def mapping_users(chunk):
    mapped_users = {}
    for row in chunk:
        if row['country'] == 'LT':
            mapped_users[row['id']] = 'LT'
    return mapped_users


def mapping_clicks(mapped_users, chunk):
    mapped_clicks = []
    for row in chunk:
        user_id = row['user_id']
        if user_id in mapped_users:
            filtered_row = {key: value for key, value in row.items() if key != 'screen'}
            mapped_clicks.append(filtered_row)
    return mapped_clicks


if __name__ == '__main__':
    # Fill in clicks_file, users_file, num_of_chunks_clicks and num_of_chunks_users manually
    # (make sure the file is placed in correct directory):
    clicks_file = 'part-004.csv'
    users_file = 'part-001.csv'
    num_of_chunks_clicks = 5
    num_of_chunks_users = 5

    clicks_dir_path = 'data/clicks'
    clicks_file_path = os.path.join(clicks_dir_path, clicks_file)
    if not os.path.exists(clicks_dir_path):
        os.makedirs(clicks_dir_path)

    users_dir_path = 'data/users'
    users_file_path = os.path.join(users_dir_path, users_file)
    if not os.path.exists(users_dir_path):
        os.makedirs(users_dir_path)

    map_reduce(clicks_file_path, users_file_path, num_of_chunks_clicks, num_of_chunks_users)
