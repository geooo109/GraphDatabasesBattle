Run the script: 
	python3 main_neo4j_benchmark.py flags

flags:
	-s [seed_path] (default: /home/geooo109/Desktop/m151/graph_database_benchmark/neo4j/benchmark_run/seeds_0.3/)
	-n [number of iterations for the benchmarks (default: 3)]
	-t [type of the query {is, ic, bi, mib, all} (default: all)]
	-d [db_name (default: "graph.db")]
	

Note:
Do not forger to change the output file in the main_neo4j_benchmark.py file
