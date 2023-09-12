import csv
import adding_processor_number_to_source
import multiprocessing
import os
import time

def split_to_list_by_workers(file, workers):
    manager = multiprocessing.Manager()
    chunk_global_names_list = []

    for i in range(1, workers + 1):
        tier_list = manager.list()
        chunk_global_names_list.append(tier_list)

    with open(file, 'r', newline='', encoding='utf-8') as in_file:
        reader = csv.reader(in_file)
        header_skipped = False  # Flag to indicate whether the header has been skipped

        for row in reader:

            if not header_skipped:
                header_skipped = True
                continue  # Skip processing the header row

            tier_list = chunk_global_names_list[int(row[-1]) - 1]
            tier_list.append(row)

    return chunk_global_names_list


def process_chunk(chunk):

    for value in chunk:
        time.sleep(0.1)  # Add a 1-second delay
        print(value)
        print(f'CPU {multiprocessing.current_process().name}')

def print_records(file):

    with open(file, 'r', newline='', encoding='utf-8') as in_file:
        reader = csv.reader(in_file)
        header_skipped = False  # Flag to indicate whether the header has been skipped

        for row in reader:

            if not header_skipped:
                header_skipped = True
                continue  # Skip processing the header row

            time.sleep(0.1)  # Add a 0.1-second delay
            print(row)


if __name__ == '__main__':

    workers = 3  # define number of processors (workers)
    filename = 'airline_dataset.csv'  # file name with extension

    current_dir = os.path.dirname(__file__)
    file = os.path.join(current_dir, f'sources/{filename}')

    print('One thread process')
    # print records without parallel and distributed method
    # print_records(file)

    print('Parallel & Distributed process')
    # tagging which process works on which records
    adding_processor_number_to_source.main(workers, filename)

    # first process that works on one file as one thread in order to prepare the data for the others functions to work as a distributed and parallel
    chunks = split_to_list_by_workers(file, workers)
    print(chunks)

    pool = multiprocessing.Pool()
    processed_data = pool.map(process_chunk, chunks)
    pool.close()
    pool.join()
