import mathdb_pb2_grpc, math_pb2
import grpc
import sys

channel = grpc.insecure_channel("localhost:5440")

for i in sys.argv:
    print(i)

# for csv in sys.argv
# create a thread
# loop over rows in csv file
# how to read in each line of csv
# collect number of hits and misses

# add up hits and misses across all csv files
# main thread calls join when all 3 threads are finished
# print hit rate
