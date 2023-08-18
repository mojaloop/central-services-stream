#!/bin/bash

# Get the current date and time
now=$(date +"%Y%m%d-%Hh%Mm%Ss")

echo "Cleaning results in ./results"

# Run npm start and pipe the output to a file with the current date and time
rm ./results/*.log

echo "Cleanup complete!"
