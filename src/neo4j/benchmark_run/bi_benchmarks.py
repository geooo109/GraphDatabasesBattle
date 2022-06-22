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

def run_BI_query(query, params, iter_num, query_name, params_dict):
    total_time = 0.0
    report = "\n---------- NEO4J " + query_name + " Time: " + str(datetime.datetime.now()) + "  ----------\n"        
    for i in range(0, iter_num):
        start = timer()
        res = query(*params)
        end = timer() 
        exe_time = end - start
        if i != 0 :
            total_time += exe_time
        line = str(i + 1) + "," +  query_name + "," + str(params_dict) + "," + str(exe_time) + " seconds"
        #print(line)
        report += line + "\n"
    report += "summary," + query_name + "," + str(params_dict) + "," + str(total_time/iter_num) + " seconds"
    return report

def run_BI(output_file, bi_name, bi_size, seed_path, iter_num, db_name):
    if not os.path.exists(os.path.dirname(output_file)):
        try:
            os.makedirs(os.path.dirname(output_file))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    param_parser = ParamsParser()
    query_runner = Neo4jQueryRunner();

    # loop through all the queries
    for q_num in range(7, bi_size + 1):
        curr_query_name = bi_name + "_" + str(q_num)
        
        params = param_parser.parse_params(bi_name, seed_path, q_num)
        
        print("\nQuery: " + curr_query_name + " - Params: " + str(params))
        
        if params is None:
            return False

        report = None
        param_pass = []
        if q_num == 1:
            param_pass.append(params["date"])
            report = run_BI_query(query_runner.bi_1, param_pass, iter_num, curr_query_name, params)
        elif q_num == 2:
            param_pass.append(params["startDate"])
            param_pass.append(params["endDate"])
            param_pass.append(params["country1Name"])
            param_pass.append(params["country2Name"])
            report = run_BI_query(query_runner.bi_2, param_pass, iter_num, curr_query_name, params)
        elif q_num == 3:
            param_pass.append(params["year"])
            param_pass.append(params["month"])
            report = run_BI_query(query_runner.bi_3, param_pass, iter_num, curr_query_name, params)
        elif q_num == 4:
            param_pass.append(params["country"])
            param_pass.append(params["tagClass"])
            report = run_BI_query(query_runner.bi_4, param_pass, iter_num, curr_query_name, params)
        elif q_num == 5:
            param_pass.append(params["country"])
            report = run_BI_query(query_runner.bi_5, param_pass, iter_num, curr_query_name, params)
        elif q_num == 6:
            param_pass.append(params["tag"])
            report = run_BI_query(query_runner.bi_6, param_pass, iter_num, curr_query_name, params)
        elif q_num == 7:
            param_pass.append(params["tag"])
            report = run_BI_query(query_runner.bi_7, param_pass, iter_num, curr_query_name, params)
        elif q_num == 8:
            param_pass.append(params["tag"])
            report = run_BI_query(query_runner.bi_8, param_pass, iter_num, curr_query_name, params)
        elif q_num == 9:
            param_pass.append(params["tagClass1"])
            param_pass.append(params["tagClass2"])
            param_pass.append(params["threshold"])
            report = run_BI_query(query_runner.bi_9, param_pass, iter_num, curr_query_name, params)
        elif q_num == 10:
            param_pass.append(params["tag"])
            param_pass.append(params["date"])   
            report = run_BI_query(query_runner.bi_10, param_pass, iter_num, curr_query_name, params)
        elif q_num == 11:
            param_pass.append(params["country"])
            param_pass.append(params["blackList"])   
            report = run_BI_query(query_runner.bi_11, param_pass, iter_num, curr_query_name, params)
        elif q_num == 12:
            param_pass.append(params["date"])
            param_pass.append(params["likeThreshold"])   
            report = run_BI_query(query_runner.bi_12, param_pass, iter_num, curr_query_name, params)
        elif q_num == 13:
            param_pass.append(params["country"])
            report = run_BI_query(query_runner.bi_13, param_pass, iter_num, curr_query_name, params)
        elif q_num == 14:
            param_pass.append(params["startDate"])
            param_pass.append(params["endDate"])   
            report = run_BI_query(query_runner.bi_14, param_pass, iter_num, curr_query_name, params)
        elif q_num == 15:
            param_pass.append(params["country"])
            report = run_BI_query(query_runner.bi_15, param_pass, iter_num, curr_query_name, params)
        elif q_num == 16:
            param_pass.append(params["personId"])
            param_pass.append(params["country"])
            param_pass.append(params["tagClass"]) 
            param_pass.append(params["minPathDistance"]) 
            param_pass.append(params["maxPathDistance"]) 
            report = run_BI_query(query_runner.bi_16, param_pass, iter_num, curr_query_name, params)
        elif q_num == 17:
            param_pass.append(params["country"])
            report = run_BI_query(query_runner.bi_17, param_pass, iter_num, curr_query_name, params)
        elif q_num == 18:
            param_pass.append(params["date"])
            param_pass.append(params["lengthThreshold"])
            param_pass.append(params["languages"])   
            report = run_BI_query(query_runner.bi_18, param_pass, iter_num, curr_query_name, params)
        elif q_num == 19:
            param_pass.append(params["date"])
            param_pass.append(params["tagClass1"])
            param_pass.append(params["tagClass2"])   
            report = run_BI_query(query_runner.bi_19, param_pass, iter_num, curr_query_name, params)
        elif q_num == 20:
            param_pass.append(params["tagClasses"])
            report = run_BI_query(query_runner.bi_20, param_pass, iter_num, curr_query_name, params)
        elif q_num == 21:
            param_pass.append(params["country"])
            param_pass.append(params["endDate"])   
            report = run_BI_query(query_runner.bi_21, param_pass, iter_num, curr_query_name, params)
        elif q_num == 22:
            param_pass.append(params["country1"])
            param_pass.append(params["country2"])   
            report = run_BI_query(query_runner.bi_22, param_pass, iter_num, curr_query_name, params)
        elif q_num == 23:
            param_pass.append(params["country"])
            report = run_BI_query(query_runner.bi_23, param_pass, iter_num, curr_query_name, params)
        elif q_num == 24:
            param_pass.append(params["tagClass"])
            report = run_BI_query(query_runner.bi_24, param_pass, iter_num, curr_query_name, params)
        elif q_num == 25:
            param_pass.append(params["person1Id"])
            param_pass.append(params["person2Id"])   
            param_pass.append(params["startDate"])
            param_pass.append(params["endDate"]) 
            report = run_BI_query(query_runner.bi_25, param_pass, iter_num, curr_query_name, params)

        ofile = open(output_file + curr_query_name + "_" + db_name + "_NEO4Jresults.txt", 'a')        
        ofile.write(report)
        ofile.close()
        print (report)
    
    return True