# raw data path
#export RAW_DATA_DIR=/home/geooo109/Desktop/m151/graph_database_benchmark/datasets/tigergraph_datasets/snb-0.3
schema=setup_schema.gsql

. ./path.sh

gsql $schema 

t0=$(date +%s%N)
bash load_data.sh
tn=$(date +%s%N)
t=$((($tn - $t0)/1000000))
echo $t $RAW_DATA_DIR
echo $t $RAW_DATA_DIR >> ./loading.out
du -sb /home/tigergraph/tigergraph/gstore/
du -sb /home/tigergraph/tigergraph/gstore/ >> ./loading.out
echo "------------------------------completed load data---------------------------------------"
