#!/bin/sh

read -p 'Mysql Username: ' usr &&
read -p 'Mysql Password: ' pw &&
spark-submit --jars /get-data/mysql-connector-java-5.1.49-bin.jar /get-data/script.py $usr $pw
