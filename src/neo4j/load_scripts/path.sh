#!/bin/bash
### java path for the neo4j
export JAVA_HOME=ADD_HERE

### change to raw data file folder
export LDBC_SNB_DATA_DIR=ADD_HERE
### change db name if you want to keep old data
export NEO4J_DB_NAME=ldbc_snb.db

### somehow LDBC SNB datagen doesn't get any benefit from multithreads. fix it to the single file for each vertex/edge
export LDBC_SNB_DATA_POSTFIX=_0_0.csv
### environment variables for neo4j
export NEO4J_HOME=ADD_HERE
export NEO4J_DB_DIR=$NEO4J_HOME/data/databases

