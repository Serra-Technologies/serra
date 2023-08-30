#!/usr/bin/bash

# only run this script from the root directory of the serra repository
./scripts/forward_connect.sh &

export SPARK_REMOTE='localhost:9999'