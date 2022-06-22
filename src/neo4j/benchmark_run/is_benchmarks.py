
import random
import os
import datetime
from timeit import default_timer as timer

import sys, logging, time
from datetime import timedelta
from json import loads
from tracemalloc import start

from yaml import parse

from query_runner import *
from config import *
from params_parser import *

def run_IS_query(query, param, iter_num, query_name):
    total_time = 0.0
    report = "\n---------- NEO4J " + query_name + " Time: " + str(datetime.datetime.now()) + "  ----------\n"        
    for i in range(0, iter_num):
        start = timer()
        query(list(param.values())[0])
        end = timer()
        exe_time = end - start
        if i != 0 :
            total_time += exe_time
        line = str(i + 1) + "," +  query_name + "," + str(param) + "," + str(exe_time) + " seconds"
        print(line)
        report += line + "\n"
    report += "summary," + query_name + "," + str(param) + "," + str(total_time/iter_num) + " seconds"
    return report

def run_IS(output_file, is_name, is_size, seed_path, iter_num, db_name):
    if not os.path.exists(os.path.dirname(output_file)):
        try:
            os.makedirs(os.path.dirname(output_file))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    param_parser = ParamsParser()
    query_runner = Neo4jQueryRunner();
    print(output_file)
    # loop through all the queries
    for q_num in range(1, is_size + 1):
        curr_is_query_name = is_name + "_" + str(q_num)
        
        param = param_parser.parse_params(is_name, seed_path, q_num)
        print(param)
        if param is None:
            return False

        report = None
        if q_num == 1:
            report = run_IS_query(query_runner.i_short_1, param, iter_num, curr_is_query_name)
        elif q_num == 2:
            report = run_IS_query(query_runner.i_short_2, param, iter_num, curr_is_query_name)
        elif q_num == 3:
            report = run_IS_query(query_runner.i_short_3, param, iter_num, curr_is_query_name)
        elif q_num == 4:
            report = run_IS_query(query_runner.i_short_4, param, iter_num, curr_is_query_name)
        elif q_num == 5:
            report = run_IS_query(query_runner.i_short_5, param, iter_num, curr_is_query_name)
        elif q_num == 6:
            report = run_IS_query(query_runner.i_short_6, param, iter_num, curr_is_query_name)
        elif q_num == 7:
            report = run_IS_query(query_runner.i_short_7, param, iter_num, curr_is_query_name)

        ofile = open(output_file + curr_is_query_name + "_" + db_name + "_NEO4Jresults.txt", 'a')        
        ofile.write(report)
        ofile.close()
        print (report)
    
    return True
