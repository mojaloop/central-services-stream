#!/bin/bash

# Get the current date and time
now=$(date +"%Y%m%d-%H%M%S")

# Run npm start and pipe the output to a file with the current date and time
npm start > "$now.log"
