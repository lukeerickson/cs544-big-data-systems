FROM p5-base
CMD mhdfs boss -fs hdfs://nn:900 &&\
sh -c "/spark-3.5.1-bin-hadoop3/sbin/start-master.sh && sleep infinity"
