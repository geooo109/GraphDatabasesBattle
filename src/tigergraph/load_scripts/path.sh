#!/bin/bash

export RAW_DATA_DIR=ADD_HERE
export LDBC_SNB_DATA_DIR=ADD_HERE

export POSTFIX=_0_0.csv

# numThreads specified in ldbc_snb_datagen/params.ini
export TOTAL_FILE_NUMBER=1

echo $LDBC_SNB_DATA_DIR
echo $RAW_DATA_DIR
