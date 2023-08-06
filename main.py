import glob
import os
import heapq as hq


def read_logs(files):
    sorted_first_logs = []
    file_handlers = {}

    for file_path in files:
        file_name = os.path.basename(file_path)
        file = open(file_path, 'r+')
        file_handlers[file_name] = file
        timestamp, log_data = next(file).strip().split(',')
        sorted_first_logs.append((timestamp, file_name, log_data))

    return sorted_first_logs, file_handlers


def get_next_log(file_handlers, file_name):
    return file_handlers[file_name].readline().strip().split(',')


def print_logs(file_handlers, sorted_first_logs):
    while len(sorted_first_logs) > 1:
        timestamp, file_name, log_data = hq.heappop(sorted_first_logs)  # log to insert
        print(f'{timestamp}, {log_data}')

        next_log_in_heap = sorted_first_logs[0]

        next_log_in_file = get_next_log(file_handlers, file_name)
        while len(next_log_in_file) > 1 and next_log_in_file[0] < next_log_in_heap[0]:
            print(','.join(next_log_in_file))
            next_log_in_file = get_next_log(file_handlers, file_name)

        if len(next_log_in_file[0]) != 0:
            hq.heappush(sorted_first_logs, (next_log_in_file[0], file_name, next_log_in_file[1]))


def close_file_handlers(file_handlers):
    for file in file_handlers.values():
        file.close()


def main():
    files = glob.glob('temp/*.log')

    sorted_first_logs, file_handlers = read_logs(files)
    hq.heapify(sorted_first_logs)
    print_logs(file_handlers, sorted_first_logs)

    close_file_handlers(file_handlers)


if __name__ == '__main__':
    main()
