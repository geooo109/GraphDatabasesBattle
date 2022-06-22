class ParamsParser:
    def parse_params(self, bench_name, seed_path, query_num):
        file_path = seed_path + bench_name + "_" + str(query_num) + ".txt"
        params = dict()
        with open(file_path, 'r') as seed_file:
            for line in seed_file:
                split_line = line.split("\n")[0]
                if split_line != "":
                    params[split_line.replace(":", "").strip()] = seed_file.readline().replace("\n", "")
            return params