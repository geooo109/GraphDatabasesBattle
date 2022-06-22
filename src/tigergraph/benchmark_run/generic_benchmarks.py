import os
import datetime
from pickle import NONE
from timeit import default_timer as timer

import json

from yaml import parse

from tigergraph_query_runner import *
from tigergraph_params_parser import *

DATE_STYLE = ["P_TIME", "T_TIME"]

def generate_param_dict(data_dict, query_type, query_num, date_style):
    params = None
    # is queries
    if query_type == "is":
        params = data_dict
    # ic queries
    elif query_type == "ic":
        if query_num == 1:
            params = data_dict
        elif query_num == 2:
            if date_style == DATE_STYLE[0]:
                max_date = datetime.datetime.strptime(data_dict["maxDate"][:-3], "%Y%m%d%H%M%S")
            elif date_style == DATE_STYLE[1]:
                max_date = datetime.datetime.fromtimestamp(int(data_dict["maxDate"])/1000).strftime("%Y-%m-%d %H:%M:%S")
            else:
                return None
            params = {"personId":data_dict["personId"], "maxDate":max_date}
        elif query_num == 3:
            if date_style == DATE_STYLE[0]:
                start_date = datetime.datetime.strptime(data_dict["startDate"][:-3], "%Y%m%d%H%M%S")
            elif date_style == DATE_STYLE[1]:
                start_date = datetime.datetime.fromtimestamp(int(data_dict["startDate"])).strftime("%Y-%m-%d %H:%M:%S")
            else: 
                return None
            params = {"personId":data_dict["personId"], "startDate":start_date, "durationDays":data_dict["duration"], "countryXName":data_dict["countryX"], "countryYName":data_dict["countryY"]}
        elif query_num == 4:
            if date_style == DATE_STYLE[0]:
                start_date = datetime.datetime.strptime(data_dict["startDate"][:-3], "%Y%m%d%H%M%S")
            elif date_style == DATE_STYLE[1]:
                start_date = datetime.datetime.fromtimestamp(int(data_dict["startDate"])/1000).strftime("%Y-%m-%d %H:%M:%S")
            else:
                return None
            params = {"personId":data_dict["personId"], "startDate":start_date, "durationDays":data_dict["durationDays"]}
        elif query_num == 5:
            if date_style == DATE_STYLE[0]:
                min_date = datetime.datetime.strptime(data_dict["minDate"][:-3], "%Y%m%d%H%M%S")
            elif date_style == DATE_STYLE[1]:
                min_date = datetime.datetime.fromtimestamp(int(data_dict["minDate"])/1000).strftime("%Y-%m-%d %H:%M:%S")
            else:
                return None
            params = {"personId":data_dict["personId"], "minDate":min_date}
        elif query_num == 6:
            params = {"personId":data_dict["personId"], "tagName" : data_dict["tagClass"]}
        elif query_num == 7:
            params = {"personId":data_dict["personId"]}
        elif query_num == 8:
            params = {"personId":data_dict["personId"]}
        elif query_num == 9:
            if date_style == DATE_STYLE[0]:
                max_date = datetime.datetime.strptime(data_dict["maxDate"][:-3], "%Y%m%d%H%M%S")
            elif date_style == DATE_STYLE[1]:
                max_date = datetime.datetime.fromtimestamp(int(data_dict["maxDate"])/1000).strftime("%Y-%m-%d %H:%M:%S")
            else:
                return NONE
            params = {"personId":data_dict["personId"], "maxDate":max_date}
        elif query_num == 10:
            month = int(data_dict["month"])
            next_month = (month + 1) if month < 12 else 1
            params = {"personId":data_dict["personId"], "month":str(month), "nextMonth":str(next_month)}
        elif query_num == 11:
            params = {"personId":data_dict["personId"], "countryName":data_dict["countryName"], "workFromYear":data_dict["workFromYear"]}
        elif query_num == 12:
            params = {"personId":data_dict["personId"], "tagClassName":data_dict["tagClassName"]}
        elif query_num == 13:
            params = {"person1Id":data_dict["person1Id"], "person2Id":data_dict["person2Id"]}
        #elif query_num == 14:
        #    params = {"person1Id":data_dict["person1Id"], "person2Id":data_dict["person2Id"]}

    # bi queries
    elif query_type == "bi":
        if query_num == 1:
            if date_style == DATE_STYLE[0]:
                max_date = datetime.datetime.strptime(data_dict["date"][:-3], "%Y%m%d%H%M%S")
            elif date_style == DATE_STYLE[1]:
                max_date = datetime.datetime.fromtimestamp(int(data_dict["date"])/1000).strftime("%Y-%m-%d %H:%M:%S")
            else:
                return None
            params = {"maxDate":max_date}
        elif query_num == 2:
            if date_style == DATE_STYLE[0]:
                start_date = datetime.datetime.strptime(data_dict["startDate"][:-3], "%Y%m%d%H%M%S")
                end_date = datetime.datetime.strptime(data_dict["endDate"][:-3], "%Y%m%d%H%M%S")
            elif date_style == DATE_STYLE[1]:
                start_date = datetime.datetime.fromtimestamp(int(data_dict["startDate"])/1000).strftime("%Y-%m-%d %H:%M:%S")
                end_date = datetime.datetime.fromtimestamp(int(data_dict["endDate"])/1000).strftime("%Y-%m-%d %H:%M:%S")
            else:
                return None
            params = {"startDate":start_date, "endDate":end_date, "country1Name":data_dict["country1Name"], "country2Name":data_dict["country2Name"]}
        elif query_num == 3:
            params = {"year1":data_dict["year"], "month1":data_dict["month"]}
        elif query_num == 4:
            params = {"tagClassName":data_dict["tagClass"], "countryName":data_dict["country"]}
        elif query_num == 5:
            params = {"countryName":data_dict["country"]}
        elif query_num == 6:
            params = {"tagName":data_dict["tag"]}
        elif query_num == 7:
            params = {"tagName":data_dict["tag"]}
        elif query_num == 8:
            params = {"tagName":data_dict["tag"]}
        elif query_num == 9:
            params = {"tagClass1Name":data_dict["tagClass1"], "tagClass2Name":data_dict["tagClass2"], "threshold":data_dict["threshold"]}
        elif query_num == 10:
            if date_style == DATE_STYLE[0]:
                min_date = datetime.datetime.strptime(data_dict["date"][:-3], "%Y%m%d%H%M%S")
            elif date_style == DATE_STYLE[1]:
                min_date = datetime.datetime.fromtimestamp(int(data_dict["date"])/1000).strftime("%Y-%m-%d %H:%M:%S")
            else:
                return None
            params = {"tagName":data_dict["tag"], "minDate":min_date}
        elif query_num == 11:
            params = {"countryName":data_dict["country"], "blacklist":data_dict["blackList"].replace("[", "").replace("]", "").split(",")}
        elif query_num == 12:
            if date_style == DATE_STYLE[0]:
                date = datetime.datetime.strptime(data_dict["date"][:-3], "%Y%m%d%H%M%S")
            elif date_style == DATE_STYLE[1]:
                date = datetime.datetime.fromtimestamp(int(data_dict["date"])/1000).strftime("%Y-%m-%d %H:%M:%S")
            else:
                return None
            params = {"minDate":date, "likeThreshold":data_dict["likeThreshold"]}
        elif query_num == 13:
            params = {"countryName":data_dict["country"]}
        elif query_num == 14:
            if date_style == DATE_STYLE[0]:
                start_date = datetime.datetime.strptime(data_dict["startDate"][:-3], "%Y%m%d%H%M%S")
                end_date = datetime.datetime.strptime(data_dict["endDate"][:-3], "%Y%m%d%H%M%S")
            elif date_style == DATE_STYLE[1]:
                start_date = datetime.datetime.fromtimestamp(int(data_dict["startDate"])/1000).strftime("%Y-%m-%d %H:%M:%S")
                end_date = datetime.datetime.fromtimestamp(int(data_dict["endDate"])/1000).strftime("%Y-%m-%d %H:%M:%S")
            else:
                return None
            params = {"startDate":start_date, "endDate":end_date}
        elif query_num == 15:
            params = {"countryName":data_dict["country"]}
        elif query_num == 16:
            params = {"personId":data_dict["personId"], "countryName":data_dict["country"], "tagClassName":data_dict["tagClass"], "minPathDistance":data_dict["minPathDistance"], "maxPathDistance":data_dict["maxPathDistance"]}
        elif query_num == 17:
            params = {"countryName":data_dict["country"]}
        elif query_num == 18:
            if date_style == DATE_STYLE[0]:
                min_date = datetime.datetime.strptime(data_dict["date"][:-3], "%Y%m%d%H%M%S")
            elif date_style == DATE_STYLE[1]:
                min_date = datetime.datetime.fromtimestamp(int(data_dict["date"])/1000).strftime("%Y-%m-%d %H:%M:%S")
            else:
                return None
            params = {"minDate":min_date, "lengthThreshold":data_dict["lengthThreshold"], "languages":data_dict["languages"].replace("[", "").replace("]", "").split(",")}
        elif query_num == 19:
            if date_style == DATE_STYLE[0]:
                min_date = datetime.datetime.strptime(data_dict["date"][:-3], "%Y%m%d%H%M%S")
            elif date_style == DATE_STYLE[1]:
                min_date = datetime.datetime.fromtimestamp(int(data_dict["date"])/1000).strftime("%Y-%m-%d %H:%M:%S")
            else:
                return None            
            params = {"minDate":min_date, "tagClass1Name":data_dict["tagClass1"], "tagClass2Name":data_dict["tagClass2"]}
        elif query_num == 20:
            params = {"tagClassNames":data_dict["tagClasses"].replace("[", "").replace("]", "").split(",")}
        elif query_num == 21:
            if date_style == DATE_STYLE[0]:
                end_date = datetime.datetime.strptime(data_dict["endDate"][:-3], "%Y%m%d%H%M%S")
            elif date_style == DATE_STYLE[1]:
                end_date = datetime.datetime.fromtimestamp(int(data_dict["endDate"])/1000).strftime("%Y-%m-%d %H:%M:%S")
            else:
                return None   
            params = {"countryName":data_dict["country"], "endDate":end_date}
        elif query_num == 22:
            params = {"country1Name":data_dict["country1"], "country2Name":data_dict["country2"]}
        elif query_num == 23:
            params = {"countryName":data_dict["country"]}
        elif query_num == 24:
            params = {"tagClassName":data_dict["tagClass"]}
    elif query_type == "mib":
        if query_num == 1:
            params = {"personId" : data_dict["personId"], "minMessageLength":data_dict["minMessageLength"]}
        elif query_num == 2:
            params = {"personId":data_dict["personId"], "tagClassInput" : data_dict["tagClassInput"], "languageInput":data_dict["languageInput"]} 
        elif query_num == 3:
            params = {"personId":data_dict["personId"], "browserName" : data_dict["browserName"]} 
        elif query_num == 4:
            dateInput = datetime.datetime.strptime(data_dict["dateInput"][:-3], "%Y%m%d%H%M%S")
            params = {"personId":data_dict["personId"], "dateInput" : dateInput} 
        elif query_num == 5:
            params = {"personId" : data_dict["personId"]}
        elif query_num == 6:
            dateInput = datetime.datetime.strptime(data_dict["dateInput"][:-3], "%Y%m%d%H%M%S")
            params = {"personId":data_dict["personId"], "dateInput" : dateInput, "tagNameInput" : data_dict["tagNameInput"]} 
        elif query_num == 7:
            params = {"personId":data_dict["personId"], "fristNameInput":data_dict["fristNameInput"]} 
        elif query_num == 8:
            creationDateInput = datetime.datetime.strptime(data_dict["creationDateInput"][:-3], "%Y%m%d%H%M%S")
            params = {"personId":data_dict["personId"], "creationDateInput" : creationDateInput} 
        elif query_num == 9:
            inputDate = datetime.datetime.strptime(data_dict["inputDate"][:-3], "%Y%m%d%H%M%S")
            params = {"personId":data_dict["personId"], "inputDate" : inputDate}
        elif query_num == 10:            params = {"messageIdInput":data_dict["messageIdInput"]}

    return params

def run_query(query, param, iter_num, query_name):
    total_time = 0.0
    report = "\n---------- TIGERGRAPH " + query_name + " Time: " + str(datetime.datetime.now()) + "  ----------\n"        
    for i in range(0, iter_num):
        start = timer()
        result = query(query_name, param)
        end = timer()
        exe_time = end - start
        if i != 0:
            total_time += exe_time
        line = str(i + 1) + "," +  query_name + "," + str(param) + "," + str(exe_time) + " seconds"
        report += line + "\n"

        #print(line)        
        #print(json.dumps(result, indent=2))
    
    print("Results of " + query_name)
    print(json.dumps(result, indent=2))

    report += "summary," + query_name + "," + str(param) + "," + str(total_time/iter_num) + " seconds"
    return report

def run_generic(conn, output_file, query_type, query_size, seed_path, iter_num, db_name):
    if not os.path.exists(os.path.dirname(output_file)):
        try:
            os.makedirs(os.path.dirname(output_file))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    param_parser = ParamsParser()
    query_runner = TigergraphQueryRunner(conn);
    print(output_file)
    print(query_size)
    # loop through all the queries
    for q_num in range(1, query_size + 1):
        curr_is_query_name = query_type + "_" + str(q_num)
        print(curr_is_query_name)
        param = param_parser.parse_params(query_type, seed_path, q_num)
        param = generate_param_dict(param, query_type, q_num, DATE_STYLE[0])
        
        if param is None:
            continue
        report = None
        print(curr_is_query_name)
        report = run_query(query_runner.query, param, iter_num, curr_is_query_name)

        ofile = open(output_file + curr_is_query_name  + "_TIGERresults.txt", 'a')        
        ofile.write(report)
        ofile.close()
        print (report)

    return True
