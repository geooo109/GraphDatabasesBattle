
import random
import os
import datetime
import re
from timeit import default_timer as timer

from datetime import timedelta
from json import loads
from tracemalloc import start

from yaml import parse

from query_runner import *
from config import *
from params_parser import *

def run_MIB_query(query, params, iter_num, query_name, params_dict):
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
        for rres in res:
            print(rres)
        report += line + "\n"
    report += "summary," + query_name + "," + str(params_dict) + "," + str(total_time/iter_num) + " seconds"
    return report

def run_MIB(output_file, mib_name, mib_size, seed_path, iter_num, db_name):
    if not os.path.exists(os.path.dirname(output_file)):
        try:
            os.makedirs(os.path.dirname(output_file))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    param_parser = ParamsParser()
    query_runner = Neo4jQueryRunner();

    # loop through all the queries
    for q_num in range(1, mib_size + 1):
        curr_query_name = mib_name + "_" + str(q_num)

        params = param_parser.parse_params(mib_name, seed_path, q_num)
        print("\nQuery: " + curr_query_name + " - Params: " + str(params))
        if params is None:
            return False

        report = None
        param_pass = []
        if q_num == 1:
            param_pass.append(params["personId"])
            param_pass.append(params["minMessageLength"])
            report = run_MIB_query(query_runner.mib_1, param_pass, iter_num, curr_query_name, params)
        elif q_num == 2:
            param_pass.append(params["personId"])
            param_pass.append(params["tagClassInput"])
            param_pass.append(params["languageInput"])
            report = run_MIB_query(query_runner.mib_2, param_pass, iter_num, curr_query_name, params)
        elif q_num == 3:
            param_pass.append(params["personId"])
            param_pass.append(params["browserName"])
            report = run_MIB_query(query_runner.mib_3, param_pass, iter_num, curr_query_name, params)
        elif q_num == 4:
            param_pass.append(params["personId"])
            param_pass.append(params["dateInput"])
            report = run_MIB_query(query_runner.mib_4, param_pass, iter_num, curr_query_name, params)
        elif q_num == 5:
            param_pass.append(params["personId"])
            report = run_MIB_query(query_runner.mib_5, param_pass, iter_num, curr_query_name, params)
        elif q_num == 6:
            param_pass.append(params["personId"])
            param_pass.append(params["dateInput"])
            param_pass.append(params["tagNameInput"])
            report = run_MIB_query(query_runner.mib_6, param_pass, iter_num, curr_query_name, params)
        elif q_num == 7:
            param_pass.append(params["personId"])
            param_pass.append(params["fristNameInput"])
            report = run_MIB_query(query_runner.mib_7, param_pass, iter_num, curr_query_name, params)
        elif q_num == 8:
            param_pass.append(params["personId"])
            param_pass.append(params["creationDateInput"])
            report = run_MIB_query(query_runner.mib_8, param_pass, iter_num, curr_query_name, params)
        elif q_num == 9:
            param_pass.append(params["personId"])
            param_pass.append(params["inputDate"])
            report = run_MIB_query(query_runner.mib_9, param_pass, iter_num, curr_query_name, params)
        elif q_num == 10:
            param_pass.append(params["messageIdInput"])   
            report = run_MIB_query(query_runner.mib_10, param_pass, iter_num, curr_query_name, params)
        ofile = open(output_file + curr_query_name + "_" + db_name + "_NEO4Jresults.txt", 'a')        
        ofile.write(report)
        ofile.close()
        print (report)
    
    return True
