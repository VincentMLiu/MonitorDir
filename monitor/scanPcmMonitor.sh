#!/bin/bash

scanDir='/fraud/ftpDir/ftp_sk/records'
todayDate=`date -d "today" +%Y%m%d`
source /etc/profile

pcmCount=`find ${scanDir} -name "${todayDate}*.pcm" -print |wc -l`
echo "[`date '+%Y-%m-%d %H:%M:%S'`] find ${scanDir} -name "${todayDate}*.pcm" -print |wc -l" >> /home/monitorFtp/monitor/monitor-monitor.log
echo "[`date '+%Y-%m-%d %H:%M:%S'`] pcmCount = ["${pcmCount}"]" >> /home/monitorFtp/monitor/monitor-monitor.log
loadingCount=`find ${scanDir} -name "${todayDate}*.loading" -print |wc -l`
echo "[`date '+%Y-%m-%d %H:%M:%S'`] find ${scanDir} -name "${todayDate}*.loading" -print |wc -l" >> /home/monitorFtp/monitor/monitor-monitor.log
echo "[`date '+%Y-%m-%d %H:%M:%S'`] loadingCount = ["${loadingCount}"]" >> /home/monitorFtp/monitor/monitor-monitor.log

if [ ${loadingCount} -ge "5000" ] || [ ${pcmCount} -ge "3000" ]
then
 echo "[`date '+%Y-%m-%d %H:%M:%S'`] loadingCount=${loadingCount} or pcmCount=${pcmCount} are reaching the Threshold(5000). Starting the re-start motion..." >> /home/monitorFtp/monitor/re-boot-monitor.log
 monitorPid=$(jcmd |grep "MonitorDirUpload2FtpPartitionedQueueScheduled"|awk '{print $1}')
 echo "[`date '+%Y-%m-%d %H:%M:%S'`] monitor pid is "${monitorPid} >> /home/monitorFtp/monitor/re-boot-monitor.log

 kill -15 ${monitorPid}
 echo "[`date '+%Y-%m-%d %H:%M:%S'`] kill -15 ${monitorPid}" >> /home/monitorFtp/monitor/re-boot-monitor.log
 sleep 1m

 sh /home/monitorFtp/HandRecoveryLoadingADay.sh >> /home/monitorFtp/monitor/re-boot-monitor.log
 echo "[`date '+%Y-%m-%d %H:%M:%S'`] start HandRecoveryLoadingADay.sh" >> /home/monitorFtp/monitor/re-boot-monitor.log
 sleep 3m

 sh /home/monitorFtp/startMonitorDirPartitionedScheduled.sh
 echo "[`date '+%Y-%m-%d %H:%M:%S'`] restart startMonitorDirPartitionedScheduled.sh" >> /home/monitorFtp/monitor/re-boot-monitor.log
 newPid=$(jcmd |grep "MonitorDirUpload2FtpPartitionedQueueScheduled"|awk '{print $1}')
 echo "[`date '+%Y-%m-%d %H:%M:%S'`] new pid is ${newPid} "  >> /home/monitorFtp/monitor/re-boot-monitor.log
else
 echo "[`date '+%Y-%m-%d %H:%M:%S'`] no problem" >> /home/monitorFtp/monitor/monitor-monitor.log
fi

