import os
import csv
import math

def read_csv_file(file):
    with open(file, 'r', newline='', encoding='utf-8') as in_file:
        reader = csv.reader(in_file)
        for row in reader:
            print(row)

def chunk_by_records_number(file, workers):
    rule_description = 'Dividing the total rows of the file in the number of workers'
    chunk_title = 'by_rows_number'

    with open(file, 'r', newline='', encoding='utf-8') as in_file:
        reader = csv.reader(in_file)
        row_counter = sum(1 for row in reader)

    records_per_worker = int(math.ceil(row_counter / workers))

    chunk_dict = {
        "title": chunk_title,
        "workers_num": workers,
        "rule": records_per_worker,
        "description": rule_description
    }

    return chunk_dict

def adding_processor_tier(file, chunk_dict):

    row_counter = 0
    worker = 1
    rule = chunk_dict['rule']

    with open(file, 'r+', newline='', encoding='utf-8') as in_file:
        csvreader = csv.reader(in_file)
        csvwriter = csv.writer(in_file)

        rows = list(csvreader)  # Read all rows into a list

        for row in rows:

            row_counter += 1

            if row_counter == 1: # only for first row add title
                row.append(chunk_dict['title'])
                continue

            if chunk_dict['title'] == 'by_rows_number':
                if row_counter >= rule:
                    rule += row_counter
                    worker += 1

                row.append(worker)  # Append the worker value for each row

            elif chunk_dict['title'] == 'by_date':
                print('create a function that splits the records by date column')

            else:
                print(3)

        in_file.seek(0)  # Move the file pointer to the beginning
        in_file.truncate()  # Clear the file content

        csvwriter.writerows(rows)  # Write the modified rows back to the file


def main(workers, filename_with_extension):

    current_dir = os.path.dirname(__file__)
    file = os.path.join(current_dir, f'sources/{filename_with_extension}')

    # reading csv file and print its records
    # read_csv_file(file) #####################

    # defining how to chunk the records (each chunk gets own processor tier)
    # the tier_rule is a dictionary
    tier_rule = chunk_by_records_number(file, workers)

    # reading source file and adding processor's tier
    adding_processor_tier(file, tier_rule)
