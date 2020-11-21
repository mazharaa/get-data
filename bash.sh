#!/bin/sh

read -p 'Mysql Username: ' usr &&
read -p 'Mysql Password: ' pw &&
read -p 'Database Target ex. localhost:3306/your_db: ' db &&
spark-submit --jars mysql-connector-java-5.1.49-bin.jar script.py $usr $pw $db
