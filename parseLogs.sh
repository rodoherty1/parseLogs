#!/bin/sh

rm ~/workspace/parseLogs/inputFiles/*
scp adaptive@10.10.9.239:/opt/pcf/analytics/log/report* ~/workspace/parseLogs/inputFiles/
js/parseLogs.js

