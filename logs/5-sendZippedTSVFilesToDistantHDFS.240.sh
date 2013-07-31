for f in `ls /hdfs/disk3/SyniverseZippedTsvFiles.240 | head -20`; do
	zcat /hdfs/disk3/SyniverseZippedTsvFiles.240/$f | ssh adaptive@10.10.9.240 "cat | /opt/pcf/analytics/hadoop/bin/hadoop dfs -put - /data/$f"
	mv /hdfs/disk3/SyniverseZippedTsvFiles.240/$f /hdfs/disk3/SyniverseZippedTsvFiles.240_sent
done

for f in `ls /hdfs/disk3/SyniverseZippedTsvFiles.240 | head -20`; do
        zcat /hdfs/disk3/SyniverseZippedTsvFiles.240/$f | ssh adaptive@10.10.9.241 "cat | /opt/pcf/analytics/hadoop/bin/hadoop dfs -put - /data/$f"
        mv /hdfs/disk3/SyniverseZippedTsvFiles.240/$f /hdfs/disk3/SyniverseZippedTsvFiles.240_sent
done

for f in `ssh adaptive@10.10.9.240 "/opt/pcf/analytics/hadoop/bin/hadoop dfs -ls /data/*.gz" | awk '{print $8}'`; do
	filename=`basename $f .gz`
	ssh adaptive@10.10.9.240 "/opt/pcf/analytics/hadoop/bin/hadoop dfs -mv /data/$filename.gz /data/$filename"
done
