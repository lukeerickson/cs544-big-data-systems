from kafka import KafkaConsumer, TopicPartition
import json, os, sys
from datetime import datetime
from collections import defaultdict
import report_pb2

def main():
    if len(sys.argv) != 3:
        print("Must use 2 partition numbers")
        sys.exit(1)
    
    partitions = [int(partition) for partition in sys.argv[1:]]
    
    broker = 'localhost:9092'
    consumer = KafkaConsumer(bootstrap_servers=[broker])
    topics = [TopicPartition("temperatures", partition) for partition in partitions]
    consumer.assign(topics)

    partition_data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for partition in partitions:
        partition_file = f"/files/partition-{partition}.json"
        if not os.path.exists(partition_file):
            initial_data = {"partition": partition, "offset": 0}
            # writes to json atomically
            write_to_json(initial_data, partition_file)
        with open(partition_file) as f:
            partition_data[partition] = json.load(f)

    try:
        while True:
            for message in consumer:
                partition = message.partition
                data = partition_data[partition]
                report = report_pb2.Report()
                report.ParseFromString(message.value)

                date = datetime.strptime(report.date, "%Y-%m-%d")
                month = date.strftime("%B")
                year = date.year
                degrees = report.degrees

                # prevents key error issues by setting default keys with values of 0
                data.setdefault(month, {}).setdefault(year, {"count": 0, "sum": 0, "start": date, "end": date})
                # check duplicate
                if date <= data[month][year].get("end", datetime.min):
                    continue

                data[month][year]["count"] += 1
                data[month][year]["sum"] += degrees
                data[month][year]["end"] = date
                
                count = len([degrees])
                if count != 0:
                    total = sum([degrees])
                    avg = total / count
                    start_date = date
                    end_date = date
                    stats = {
                            "count": count,
                            "sum": total,
                            "avg": avg,
                            "start": start_date,
                            "end": end_date
                    }
                    data[month][year].update(stats)
                partition_file = f"/files/partition-{partition}.json"
                write_to_json(data, partition_file)
                #print(str(month) + " " + str(year) + " " +  str(degrees))

    except KeyboardInterrupt:
        print("keyboard interrupt")
        consumer.close()

def write_to_json(data, file):
    temp_file = file + ".tmp"
    with open(temp_file, "w") as f:
        json.dump(data, f, indent=2, default=datetime_serializer)
    os.rename(temp_file, file)

def datetime_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()

if __name__ == "__main__":
    main()
