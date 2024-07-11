FROM p5-base
CMD hdfs worker -fs hdfs://nn:9000 &&\
sh -c "/spark-3.5.1-bin-hadoop3/sbin/start-worker.sh spark://boss:7077 -c 1 -m 512M && sleep infinity"
