#!/bin/bash

git clone https://github.com/ligurio/elle-cli.git
cd elle-cli || exit

lein deps
lein uberjar

export ELLE_JAR=$(pwd)/target/elle-cli-0.1.7-standalone.jar
