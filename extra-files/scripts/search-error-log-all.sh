#!/bin/bash

clush -w @kairos_all 'grep -ir "ERROR\|Exception" /disk3/kairos/hadoop-2.7.1/logs/*' | grep -v 'RECEIVED SIGNAL 15: SIGTERM'
clush -w @kairos_all 'grep -ir ERROR /disk3/kairos/logs/*' | grep -v 'RECEIVED SIGNAL 15: SIGTERM' | grep -v 'VolumeScanner'
