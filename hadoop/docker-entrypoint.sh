#!/bin/bash
set -e

HCONF="/opt/hadoop/etc/hadoop"
export HADOOP_USER_NAME=hadoop

# 포맷은 최초 1회만
if [ ! -d /hadoop/dfs/name/current ]; then
  echo "Formatting HDFS..."
  hdfs --config $HCONF namenode -format -force
fi

# 포그라운드로 시작
echo "Starting NameNode..."
hdfs --config $HCONF namenode &
NAMENODE_PID=$!

# safemode 해제 대기
echo "Waiting for safemode off..."
until hdfs --config $HCONF dfsadmin -fs hdfs://namenode:8020 -safemode get | grep -q 'Safe mode is OFF'; do
  sleep 5
done

# 디렉토리 자동 생성
echo "Creating initial HDFS directories..."
hdfs --config $HCONF dfs -mkdir -p /news_archive /realtime /reports
hdfs --config $HCONF dfs -chmod 777 /news_archive /realtime /reports

wait $NAMENODE_PID
