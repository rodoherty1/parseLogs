#!/bin/sh


DATE=`date +"%m-%d-%y"`

tar -cvf results/archive/results_$DATE.tar results/*.*

rm ./inputFiles/*
rm ./results/*.*
scp adaptive@10.10.9.240:/opt/pcf/analytics/log/report* ./inputFiles/
js/parseLogs.js

