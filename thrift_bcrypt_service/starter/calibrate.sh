#!/bin/bash

# Calibration script for bcrypt service
# Determines optimal log rounds parameter based on system performance

javac -cp jBCrypt-0.4/jbcrypt.jar Calibrator.java
java -cp .:jBCrypt-0.4/jbcrypt.jar Calibrator | tee calibration.txt
