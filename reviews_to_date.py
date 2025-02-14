import csv
import os
import sys
from datetime import datetime

def read_file(filename):
    real_filename = filename.replace('_reviews.csv','')
    with open(filename,'r', encoding="utf-8") as file_handle, open(real_filename + "_reviews_dates.csv",'w', newline='') as csv_write_handle:
        list = []
        count = 0
        # 1514764801 - utc for 2018 Jan
        # 1688169599 - utc for 2023 end of Jun
        #reader = csv.reader(file_handle)
        csv_writer = csv.writer(csv_write_handle)
        lines = file_handle.read().splitlines()
        count_positive = 0
        count_negative = 0
        csv_writer.writerow(["date","+ve","-ve","total"])
        current_date = datetime.fromtimestamp(1737726042).date()
        start_date = datetime.fromtimestamp(1514764801).date()
        end_date = datetime.fromtimestamp(1688169599).date()

        for line in lines:
            count += 1
            # line = file_handle.readline()
            fields = line.split(',')
            if (count<=2):
                continue
            date = int(fields[2])
            date = datetime.fromtimestamp(date).date()
            if date < start_date or date > end_date:
                continue
            if (fields[1] == "True"):
                count_positive += 1
            else:
                count_negative += 1
            if date < current_date:
                csv_writer.writerow([current_date,count_positive,count_negative,count])
                current_date = date
                count = 0
                count_negative = 0
                count_positive = 0
        return lines


if __name__ == "__main__":
    file_path = sys.argv[1]
    rows = read_file(file_path)
