#!python3 -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. report.proto

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError, TopicAlreadyExistsError
import datetime
import calendar
import time
import weather
import grpc
import report_pb2
import subprocess

#subprocess.run(['python3 -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. report.proto'])

command = [
        "python3",
        "-m",
        "grpc_tools.protoc",
        "-I=.",
        "--python_out=.",
        "--grpc_python_out=.",
        "files/report.proto"
]

subprocess.run(command)

#print("protofile successfully generated")

broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic (might not exist yet)")

time.sleep(3)

# create topic temperastures w/ 4 partitions and rf = 1
try:
    admin_client.create_topics([NewTopic(name="temperatures", num_partitions=4, replication_factor=1)])
except TopicAlreadyExistsError:
    print("Topic already exists")

print("Topics:", admin_client.list_topics())

producer = KafkaProducer(
        bootstrap_servers=[broker],
        retries=10,
        acks='all'
)

for date, degrees in weather.get_next_weather(delay_sec=0.1):
        #print(date, degrees)
        report = report_pb2.Report()
        report.date = date
        report.degrees = degrees
       
        # put date into correct format and then extract month as a string
        date_obj = datetime.datetime.strptime(report.date, "%Y-%m-%d")
        month_num = date_obj.month
        month_name = datetime.date(1900, month_num, 1).strftime('%B')

        report_bytes = report.SerializeToString()
        result = producer.send("temperatures", key=month_name.encode(), value=report_bytes)
        #print(result)
