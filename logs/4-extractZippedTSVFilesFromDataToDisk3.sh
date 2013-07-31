for f in `/opt/pcf/analytics/hadoop/bin/hadoop dfs -ls /data | awk '{print $8}'`; do
        filename=`basename $f`
        /opt/pcf/analytics/hadoop/bin/hadoop dfs -get $f - | gzip -c | ssh adaptive@10.10.9.209 "cat > /hdfs/disk3/SyniverseZippedTsvFiles/$filename.gz"
done

