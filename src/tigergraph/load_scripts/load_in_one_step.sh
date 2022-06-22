# raw data path
#export RAW_DATA_DIR=/home/geooo109/Desktop/m151/graph_database_benchmark/datasets/tigergraph_datasets/snb-0.3

. ./path.sh
echo "------------------------------completed set path----------------------------"

#bash convert_data.sh
#echo "------------------------------completed convert raw data----------------------------"

gsql setup_schema.gsql
echo "------------------------------completed setup schema---------------------------------------"

t0=$(date +%s%N)
bash load_data.sh
tn=$(date +%s%N)
t=$((($tn - $t0)/1000000))
echo $t $RAW_DATA_DIR
echo $t $RAW_DATA_DIR >> ./loading.out
du -h /home/tigergraph/tigergraph/data/gstore/
du -h /home/tigergraph/tigergraph/data/gstore/ >> ./loading.out
echo "------------------------------completed load data---------------------------------------"
