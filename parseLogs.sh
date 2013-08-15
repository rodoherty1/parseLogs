#!/bin/sh

rm ~/workspace/parseLogs/inputFiles/*
rm ~/workspace/parseLogs/results/*
scp adaptive@10.10.9.240:/opt/pcf/analytics/log/report* ~/workspace/parseLogs/inputFiles/
js/parseLogs.js

