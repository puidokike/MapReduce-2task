# Map-Reduce Data Processing
This code processes two datasets (clicks and users), filters the data based on specific conditions (user country 'LT'), and generates an output CSV file with the filtered data. The processing is done using the Map-Reduce paradigm, splitting the data into chunks and using the multiprocessing module to process the data in parallel.

### Instructions
1. Ensure that Python 3.6 or higher is installed on your machine.
2. Example CSV files have been included. You may replace it with any other file of your preference: place the CSV files you want to process in the 'data/clicks' and 'data/users' directories. Manually rewrite this part of __if __name__ == '__main__':__ block:

clicks_file: Name of the clicks dataset file.\
users_file: Name of the users dataset file.\
num_of_chunks_clicks: Number of chunks to split the clicks dataset.\
num_of_chunks_users: Number of chunks to split the users dataset.

3. The output CSV file will be located in the autocreated 'data/filtered_clicks' directory.


