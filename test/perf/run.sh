#!/bin/bash

# Get the current date and time
now=$(date +"%Y%m%d-%Hh%Mm%Ss")

echo "Starting test > $now.log"

# Run npm start and pipe the output to a file with the current date and time
npm start > "$now.log"

echo "Test complete"
