#RAW_DATA_DIR destination to store converted raw data
#export RAW_DATA_DIR=/home/geooo109/Desktop/m151/graph_database_benchmark/datasets/tigergraph_datasets/snb-0.3
#export POSTFIX=_0_0.csv

. ./path.sh
gsql -g ldbc_snb "run loading job load_ldbc_snb using 
v_person_file=\"${RAW_DATA_DIR}/person_0_0.csv\",
v_post_file=\"${RAW_DATA_DIR}/post_0_0.csv\", 
v_tag_file=\"${RAW_DATA_DIR}/tag_0_0.csv\", 
v_place_file=\"${RAW_DATA_DIR}/place_0_0.csv\",
v_comment_file=\"${RAW_DATA_DIR}/comment_0_0.csv\", 
v_forum_file=\"${RAW_DATA_DIR}/forum_0_0.csv\", 
v_organisation_file=\"${RAW_DATA_DIR}/organisation_0_0.csv\", 
v_tagclass_file=\"${RAW_DATA_DIR}/tagclass_0_0.csv\",
person_knows_person_file=\"${RAW_DATA_DIR}/person_knows_person_0_0.csv\", 
comment_replyOf_post_file=\"${RAW_DATA_DIR}/comment_replyOf_post_0_0.csv\", 
comment_replyOf_comment_file=\"${RAW_DATA_DIR}/comment_replyOf_comment_0_0.csv\", 
post_hasCreator_person_file=\"${RAW_DATA_DIR}/post_hasCreator_person_0_0.csv\", 
post_hasTag_tag_file=\"${RAW_DATA_DIR}/post_hasTag_tag_0_0.csv\", 
comment_hasCreator_person_file=\"${RAW_DATA_DIR}/comment_hasCreator_person_0_0.csv\", 
post_isLocatedIn_place_file=\"${RAW_DATA_DIR}/post_isLocatedIn_place_0_0.csv\", 
comment_hasTag_tag_file=\"${RAW_DATA_DIR}/comment_hasTag_tag_0_0.csv\", 
comment_isLocatedIn_place_file=\"${RAW_DATA_DIR}/comment_isLocatedIn_place_0_0.csv\", 
forum_containerOf_post_file=\"${RAW_DATA_DIR}/forum_containerOf_post_0_0.csv\", 
forum_hasMember_person_file=\"${RAW_DATA_DIR}/forum_hasMember_person_0_0.csv\", 
forum_hasModerator_person_file=\"${RAW_DATA_DIR}/forum_hasModerator_person_0_0.csv\", 
forum_hasTag_tag_file=\"${RAW_DATA_DIR}/forum_hasTag_tag_0_0.csv\", 
organisation_isLocatedIn_place_file=\"${RAW_DATA_DIR}/organisation_isLocatedIn_place_0_0.csv\",
person_hasInterest_tag_file=\"${RAW_DATA_DIR}/person_hasInterest_tag_0_0.csv\", 
person_isLocatedIn_place_file=\"${RAW_DATA_DIR}/person_isLocatedIn_place_0_0.csv\", 
person_likes_comment_file=\"${RAW_DATA_DIR}/person_likes_comment_0_0.csv\", 
person_likes_post_file=\"${RAW_DATA_DIR}/person_likes_post_0_0.csv\", 
person_studyAt_organisation_file=\"${RAW_DATA_DIR}/person_studyAt_organisation_0_0.csv\", 
person_workAt_organisation_file=\"${RAW_DATA_DIR}/person_workAt_organisation_0_0.csv\", 
place_isPartOf_place_file=\"${RAW_DATA_DIR}/place_isPartOf_place_0_0.csv\",
tag_hasType_tagclass_file=\"${RAW_DATA_DIR}/tag_hasType_tagclass_0_0.csv\", 
tagclass_isSubclassOf_tagclass_file=\"${RAW_DATA_DIR}/tagclass_isSubclassOf_tagclass_0_0.csv\"" 
