#!/bin/bash
# Use this script to start pika

../../output/pika -c ./conf/master/pika_master.conf
../../output/pika -c ./conf/slave/pika_slave.conf
#ensure both master and slave are ready
sleep 10