from kafka import KafkaConsumer
import report_pb2

broker = "localhost:9092"

consumer = KafkaConsumer(bootstrap_servers=[broker], group_id='debug')
consumer.subscribe(["temperatures"])

for msg in consumer:
        report = report_pb2.Report()
        report.ParseFromString(msg.value)
        msg_dict = {
                'partition': msg.partition,
                'key': msg.key.decode(),
                'date': report.date,
                'degrees': report.degrees
        }
        print(msg_dict)
