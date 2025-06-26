#!/bin/bash
java -Dconfig=/home/workspace/trucking-application/application.conf -jar /home/workspace/trucking-application/TruckingSimulation-1.0-SNAPSHOT.jar > /home/workspace/trucking-application/stedi.log 2>&1 &
