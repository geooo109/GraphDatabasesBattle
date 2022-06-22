from timeit import default_timer as timer

import sys, logging, time
from datetime import timedelta
from json import loads
from tracemalloc import start

import pyTigerGraph as tg 

from generic_benchmarks import *

# default value for arguments
DEFAULT_ITER_NUM = 3
DEFAULT_DB_NAME = "ldbc_snb.db"
DEFAULT_PATH_TO_SEEDS = "/home/geooo109/Desktop/m151/graph_database_benchmark/tigergraph/benchmark_run/seeds_0.3/"


# params for interactive short queries
IS_NAME = "Interactive Short"
IS_TYPE = "is"
IS_SIZE = 7

# params for ic queries
IC_NAME = "Interactive Complex"
IC_TYPE = "ic"
IC_SIZE = 13

# params for bi queries
BI_NAME = "Business Intelligence"
BI_TYPE = "bi"
BI_SIZE = 24

# params for my queries
MICHAS_NAME = "Michas Benchmarks"
MIB_TYPE = "mib"
MIB_SIZE = 10

ALL_TYPE = "all"

DEFAULT_OUTPUT_FILE_01 = "/home/geooo109/Desktop/m151/graph_database_benchmark/tigergraph/benchmark_run/tigergraph_output/out_0.1/"
DEFAULT_OUTPUT_FILE_03 = "/home/geooo109/Desktop/m151/graph_database_benchmark/tigergraph/benchmark_run/tigergraph_output/out_0.3/"
DEFAULT_OUTPUT_FILE_1 = "/home/geooo109/Desktop/m151/graph_database_benchmark/tigergraph/benchmark_run/tigergraph_output/out_1/"

def parse_args(argv):
    seed_path = None
    iter_num = None
    query_type = None
    db_name = None
    for i in range(1, len(argv)):
        if argv[i] == "-s":
            seed_path = argv[i + 1]
        elif argv[i] == "-n":
            iter_num = int(argv[i + 1])
        elif argv[i] == "-t":
            query_type = argv[i+1]
        elif argv[i] == "-d":
            db_name = argv[i+1]
    if seed_path is None:
        seed_path = DEFAULT_PATH_TO_SEEDS
    if iter_num is None:
        iter_num = DEFAULT_ITER_NUM
    if query_type is None:
        query_type = ALL_TYPE
    if db_name is None:
        db_name = DEFAULT_DB_NAME
    print("Input params: [seed_path:{}], [iter_num:{}], [query_type:{}]".format(seed_path, iter_num, query_type))
    return seed_path, iter_num, query_type, db_name

if __name__ == "__main__":

    # parse input flags
    seed_path, iter_num, query_type, db_name = parse_args(sys.argv)

    # connect to the tigergraph database
    conn = tg.TigerGraphConnection(
        host="http://127.0.0.1",
        username="tigergraph",
        password="tigergraph",
        graphname="ldbc_snb" 
    )
    
    #============================================
    # WARNING  :CHANGE THIS 
    OUT_FILE_SELECT = DEFAULT_OUTPUT_FILE_03

    print(OUT_FILE_SELECT)
    if query_type == IS_TYPE:
        print("Run IS Benchmarks...")
        res = run_generic(conn, OUT_FILE_SELECT, IS_TYPE, IS_SIZE, seed_path, iter_num, db_name)
        if res == False:
            print("Benchmarks IS [Failed]")
        
        print("Benchmarks IS [Finished]")

    elif query_type == IC_TYPE:
        print("Run IC Benchmarks...")
        res = run_generic(conn, OUT_FILE_SELECT, IC_TYPE, IC_SIZE, seed_path, iter_num, db_name)
        if res == False:
            print("Benchmarks IC [Failed]")
    
        print("Benchmarks IC [Finished]")
    
    elif query_type == BI_TYPE:
        print("Run BI Benchmarks...")
        res = run_generic(conn, OUT_FILE_SELECT, BI_TYPE, BI_SIZE, seed_path, iter_num, db_name)
        if res == False:
            print("Benchmarks BI [Failed]")
    
        print("Benchmarks BI [Finished]")

    elif query_type == MIB_TYPE:
        print("Run MIB Benchmarks...")
        res = run_generic(conn, OUT_FILE_SELECT, MIB_TYPE, MIB_SIZE, seed_path, iter_num, db_name)
        if res == False:
            print("Benchmarks BI [Failed]")
        print("Run MIB Benchmarks...")

    elif query_type == ALL_TYPE:
        print("Run IS + IC + BI + MIB Benchmarks...")
        res = run_generic(conn, OUT_FILE_SELECT, IS_TYPE, IS_SIZE, seed_path, iter_num, db_name)
        res = run_generic(conn, OUT_FILE_SELECT, IC_TYPE, IC_SIZE, seed_path, iter_num, db_name)
        res = run_generic(conn, OUT_FILE_SELECT, BI_TYPE, BI_SIZE, seed_path, iter_num, db_name)
        res = run_generic(conn, OUT_FILE_SELECT, MIB_TYPE, MIB_SIZE, seed_path, iter_num, db_name)
        print("Benchmarks IS + IC + BI + MIB [Finished]")

    else:
        print("Error for query type param")