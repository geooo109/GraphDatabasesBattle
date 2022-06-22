
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

def run_IC_query(query, params, iter_num, query_name, params_dict):
    total_time = 0.0
    report = "\n---------- NEO4J " + query_name + " Time: " + str(datetime.datetime.now()) + "  ----------\n"        
    for i in range(0, iter_num):
        start = timer()
        res = query(*params)
        end = timer()
        exe_time = end - start
        total_time += exe_time
        line = str(i + 1) + "," +  query_name + "," + str(params_dict) + "," + str(exe_time) + " seconds"
        print(res)
        report += line + "\n"
    report += "summary," + query_name + "," + str(params_dict) + "," + str(total_time/iter_num) + " seconds"
    return report

def run_IC(output_file, ic_name, ic_size, seed_path, iter_num, db_name):
    if not os.path.exists(os.path.dirname(output_file)):
        try:
            os.makedirs(os.path.dirname(output_file))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    param_parser = ParamsParser()
    query_runner = Neo4jQueryRunner();

    # loop through all the queries
    for q_num in range(1, ic_size + 1):
        curr_query_name = ic_name + "_" + str(q_num)

        params = param_parser.parse_params(ic_name, seed_path, q_num)
        print("\nQuery: " + curr_query_name + " - Params: " + str(params))
        if params is None:
            return False

        report = None
        param_pass = []
        if q_num == 1:
            param_pass.append(params["personId"])
            param_pass.append(params["firstName"])
            report = run_IC_query(query_runner.i_complex_1, param_pass, iter_num, curr_query_name, params)
        elif q_num == 2:
            param_pass.append(params["personId"])
            param_pass.append(params["maxDate"])
            report = run_IC_query(query_runner.i_complex_2, param_pass, iter_num, curr_query_name, params)
        elif q_num == 3:
            param_pass.append(params["personId"])
            param_pass.append(params["startDate"])
            param_pass.append(params["duration"])
            param_pass.append(params["countryX"])
            param_pass.append(params["countryY"])
            report = run_IC_query(query_runner.i_complex_3, param_pass, iter_num, curr_query_name, params)
        elif q_num == 4:
            param_pass.append(params["personId"])
            param_pass.append(params["startDate"])
            param_pass.append(params["durationDays"])
            report = run_IC_query(query_runner.i_complex_4, param_pass, iter_num, curr_query_name, params)
        elif q_num == 5:
            param_pass.append(params["personId"])
            param_pass.append(params["minDate"])
            report = run_IC_query(query_runner.i_complex_5, param_pass, iter_num, curr_query_name, params)
        elif q_num == 6:
            param_pass.append(params["personId"])
            param_pass.append(params["tagClass"])
            report = run_IC_query(query_runner.i_complex_6, param_pass, iter_num, curr_query_name, params)
        elif q_num == 7:
            param_pass.append(params["personId"])
            report = run_IC_query(query_runner.i_complex_7, param_pass, iter_num, curr_query_name, params)
        elif q_num == 8:
            param_pass.append(params["personId"])
            report = run_IC_query(query_runner.i_complex_8, param_pass, iter_num, curr_query_name, params)
        elif q_num == 9:
            param_pass.append(params["personId"])
            param_pass.append(params["maxDate"])
            report = run_IC_query(query_runner.i_complex_9, param_pass, iter_num, curr_query_name, params)
        elif q_num == 10:
            param_pass.append(params["personId"])
            param_pass.append(params["month"])   
            report = run_IC_query(query_runner.i_complex_10, param_pass, iter_num, curr_query_name, params)
        elif q_num == 11:
            param_pass.append(params["personId"])
            param_pass.append(params["countryName"])   
            param_pass.append(params["workFromYear"])   
            report = run_IC_query(query_runner.i_complex_11, param_pass, iter_num, curr_query_name, params)
        elif q_num == 12:
            param_pass.append(params["personId"])
            param_pass.append(params["tagClassName"])   
            report = run_IC_query(query_runner.i_complex_12, param_pass, iter_num, curr_query_name, params)
        elif q_num == 13:
            param_pass.append(params["person1Id"])
            param_pass.append(params["person2Id"])   
            report = run_IC_query(query_runner.i_complex_13, param_pass, iter_num, curr_query_name, params)
        elif q_num == 14:
            param_pass.append(params["person1Id"])
            param_pass.append(params["person2Id"])   
            report = run_IC_query(query_runner.i_complex_14, param_pass, iter_num, curr_query_name, params)

        ofile = open(output_file + curr_query_name + "_" + db_name + "_NEO4Jresults.txt", 'a')        
        ofile.write(report)
        ofile.close()
        print (report)
    
    return True
