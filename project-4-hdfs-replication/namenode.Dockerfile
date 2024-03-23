FROM p4-hdfs
CMD hdfs namenode -format &&\ 
hdfs namenode -D dfs.namenode.stale.datanode.interval=1000 -D dfs.namenode.heartbeat.recheck-interval=1000 -D dfs.heartbeat.interval=1000ms -D dfs.namenode.avoid.read.stale.datanode=True -fs hdfs://boss:9000
