#!/bin/bash

# Get the current date and time
now=$(date +"%Y%m%d-%Hh%Mm%Ss")

echo "Starting test > ./results/$now.log"

echo "Starting tests - $now" > "./results/$now.log"

echo "=======================================" >> "./results/$now.log"

case "$OSTYPE" in
    darwin*)
      echo "MacOS version: $(sw_vers -productVersion)"  >> "./results/$now.log"
      echo "MacOS CPU: $(sysctl -n machdep.cpu.brand_string)" >> "./results/$now.log"
      echo "MacOS Memory: $(sysctl -n hw.memsize | awk '{print $0/1073741824 " GB"}')" >> "./results/$now.log"
    ;;
    linux-gnu*)
      if [ -f /etc/os-release ]; then
        . /etc/os-release
        echo "Linux distribution: $PRETTY_NAME" >> "./results/$now.log"
      fi
      echo "Linux CPU: $(grep "model name" /proc/cpuinfo | head -n1 | awk -F: '{print $2}' | sed 's/^[ \t]*//')" >> "./results/$now.log"
      echo "Linux Memory: $(free -h | awk '/^Mem:/ {print $2}')" >> "./results/$now.log" 
    ;;
    *)
      echo "Unsupported OS" >> "./results/$now.log"
    ;;
esac

echo "=======================================" >> "./results/$now.log"

echo "docker version = $(docker version)" >> "./results/$now.log"
echo "docker compose version = $(docker compose version)" >> "./results/$now.log"

echo "=======================================" >> "./results/$now.log"

# Run npm start and pipe the output to a file with the current date and time
npm start >> "./results/$now.log"

echo "Test complete"
