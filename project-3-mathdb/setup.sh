#!/bin/bash
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/p3/autograde.py -O autograde.py
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/tester.py -O tester.py

mkdir -p workload
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/p3/workload/workload1.csv -O workload/workload1.csv
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/p3/workload/workload2.csv -O workload/workload2.csv
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/p3/workload/workload3.csv -O workload/workload3.csv
