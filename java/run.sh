#! /bin/bash
DBNAME=$1
PORT=$2
USER=$3

# Example: source ./run.sh flightDB 5432 user
java -cp lib/*:bin/ DBproject $DBNAME $PORT $USER
