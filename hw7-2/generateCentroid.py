import csv
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('K', type=int, nargs='?', default=5)
args = parser.parse_args()

K = args.K

with open('computers.csv', 'r') as fin, open('centroid.txt', 'w', encoding='utf8') as fout:
    reader = csv.reader(fin)
    next(reader)
    cnt = 0
    for row in reader:
        fout.write('{} {} {} {} {} {} {}\n'.format(
            (float(row[1]) - 949) / (5399 - 949),  # price
            (float(row[2]) - 25) / (100 - 25),  # speed
            (float(row[3]) - 80) / (2100 - 80),  # hd
            (float(row[4]) - 2) / (32 - 2),  # ram
            (float(row[5]) - 14) / (17 - 14),  # screen
            (float(row[9]) - 39) / (339 - 39),  # ads
            (float(row[10]) - 1) / (35 - 1),  # trend
        ))
        cnt += 1
        if cnt >= K:
            break
