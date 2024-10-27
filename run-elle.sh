#!/bin/bash

if [ $? -ne 2 ]; then
  echo "Usage ./run-elle.sh <edn_file> <output_directory>"
  exit 1
fi

java -jar $ELLE_JAR --model list-append "$1" --directory "$2"