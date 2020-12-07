#!/bin/bash

read -p 'MySQL Username: ' usr &&
read -sp 'MySQL Password: ' pw &&
read -p 'Your Bind-Address:port/ Ex. "localhost:3306/": ' ba &&
read -p 'Your Database Target: ' db &&
spark-submit --jars mysql-connector-java-5.1.49-bin.jar script.py $usr $pw $ba $db
