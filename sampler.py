import csv
import os
import sys
from random import sample

def read_file(filename):
    with open(filename,'r', encoding="utf-8") as file_handle:
        list = []
        count = 0
        #reader = csv.reader(file_handle)
        #skip title
        #reader.__next__()
        lines = file_handle.read().splitlines()
        for row in file_handle:
            line = file_handle.readline()
            list += line # row.encode().decode('utf-8-sig')
            # if line != "":
            #     list+=(line)
            #     count += 1
        return lines
    

if __name__ == "__main__":
    file_path = sys.argv[1]
    rows = read_file(file_path)
    for row in sample(rows, 30):
        print(row)
