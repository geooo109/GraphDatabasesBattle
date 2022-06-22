#!/bin/bash

. ./path.sh

$NEO4J_HOME/bin/neo4j-admin import --database=$NEO4J_DB_NAME \
  --id-type=INTEGER \
  --nodes:Message:Comment "${LDBC_SNB_DATA_DIR}/comment_header.csv,${LDBC_SNB_DATA_DIR}/comment${LDBC_SNB_DATA_POSTFIX}" \
  --nodes:Forum "${LDBC_SNB_DATA_DIR}/forum_header.csv,${LDBC_SNB_DATA_DIR}/forum${LDBC_SNB_DATA_POSTFIX}" \
  --nodes:Organisation "${LDBC_SNB_DATA_DIR}/organisation_header.csv,${LDBC_SNB_DATA_DIR}/organisation${LDBC_SNB_DATA_POSTFIX}" \
  --nodes:Person "${LDBC_SNB_DATA_DIR}/person_header.csv,${LDBC_SNB_DATA_DIR}/person${LDBC_SNB_DATA_POSTFIX}" \
  --nodes:Place "${LDBC_SNB_DATA_DIR}/place_header.csv,${LDBC_SNB_DATA_DIR}/place${LDBC_SNB_DATA_POSTFIX}" \
  --nodes:Message:Post "${LDBC_SNB_DATA_DIR}/post_header.csv,${LDBC_SNB_DATA_DIR}/post${LDBC_SNB_DATA_POSTFIX}" \
  --nodes:TagClass "${LDBC_SNB_DATA_DIR}/tagclass_header.csv,${LDBC_SNB_DATA_DIR}/tagclass${LDBC_SNB_DATA_POSTFIX}" \
  --nodes:Tag "${LDBC_SNB_DATA_DIR}/tag_header.csv,${LDBC_SNB_DATA_DIR}/tag${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:HAS_CREATOR "${LDBC_SNB_DATA_DIR}/comment_hasCreator_person_header.csv,${LDBC_SNB_DATA_DIR}/comment_hasCreator_person${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:IS_LOCATED_IN "${LDBC_SNB_DATA_DIR}/comment_isLocatedIn_place_header.csv,${LDBC_SNB_DATA_DIR}/comment_isLocatedIn_place${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:REPLY_OF "${LDBC_SNB_DATA_DIR}/comment_replyOf_comment_header.csv,${LDBC_SNB_DATA_DIR}/comment_replyOf_comment${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:REPLY_OF "${LDBC_SNB_DATA_DIR}/comment_replyOf_post_header.csv,${LDBC_SNB_DATA_DIR}/comment_replyOf_post${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:CONTAINER_OF "${LDBC_SNB_DATA_DIR}/forum_containerOf_post_header.csv,${LDBC_SNB_DATA_DIR}/forum_containerOf_post${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:HAS_MEMBER "${LDBC_SNB_DATA_DIR}/forum_hasMember_person_header.csv,${LDBC_SNB_DATA_DIR}/forum_hasMember_person${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:HAS_MODERATOR "${LDBC_SNB_DATA_DIR}/forum_hasModerator_person_header.csv,${LDBC_SNB_DATA_DIR}/forum_hasModerator_person${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:HAS_TAG "${LDBC_SNB_DATA_DIR}/forum_hasTag_tag_header.csv,${LDBC_SNB_DATA_DIR}/forum_hasTag_tag${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:HAS_INTEREST "${LDBC_SNB_DATA_DIR}/person_hasInterest_tag_header.csv,${LDBC_SNB_DATA_DIR}/person_hasInterest_tag${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:IS_LOCATED_IN "${LDBC_SNB_DATA_DIR}/person_isLocatedIn_place_header.csv,${LDBC_SNB_DATA_DIR}/person_isLocatedIn_place${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:KNOWS "${LDBC_SNB_DATA_DIR}/person_knows_person_header.csv,${LDBC_SNB_DATA_DIR}/person_knows_person${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:LIKES "${LDBC_SNB_DATA_DIR}/person_likes_comment_header.csv,${LDBC_SNB_DATA_DIR}/person_likes_comment${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:LIKES "${LDBC_SNB_DATA_DIR}/person_likes_post_header.csv,${LDBC_SNB_DATA_DIR}/person_likes_post${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:IS_PART_OF "${LDBC_SNB_DATA_DIR}/place_isPartOf_place_header.csv,${LDBC_SNB_DATA_DIR}/place_isPartOf_place${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:HAS_CREATOR "${LDBC_SNB_DATA_DIR}/post_hasCreator_person_header.csv,${LDBC_SNB_DATA_DIR}/post_hasCreator_person${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:HAS_TAG "${LDBC_SNB_DATA_DIR}/comment_hasTag_tag_header.csv,${LDBC_SNB_DATA_DIR}/comment_hasTag_tag${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:HAS_TAG "${LDBC_SNB_DATA_DIR}/post_hasTag_tag_header.csv,${LDBC_SNB_DATA_DIR}/post_hasTag_tag${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:IS_LOCATED_IN "${LDBC_SNB_DATA_DIR}/post_isLocatedIn_place_header.csv,${LDBC_SNB_DATA_DIR}/post_isLocatedIn_place${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:IS_SUBCLASS_OF "${LDBC_SNB_DATA_DIR}/tagclass_isSubclassOf_tagclass_header.csv,${LDBC_SNB_DATA_DIR}/tagclass_isSubclassOf_tagclass${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:HAS_TYPE "${LDBC_SNB_DATA_DIR}/tag_hasType_tagclass_header.csv,${LDBC_SNB_DATA_DIR}/tag_hasType_tagclass${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:STUDY_AT "${LDBC_SNB_DATA_DIR}/person_studyAt_organisation_header.csv,${LDBC_SNB_DATA_DIR}/person_studyAt_organisation${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:WORK_AT "${LDBC_SNB_DATA_DIR}/person_workAt_organisation_header.csv,${LDBC_SNB_DATA_DIR}/person_workAt_organisation${LDBC_SNB_DATA_POSTFIX}" \
  --relationships:IS_LOCATED_IN "${LDBC_SNB_DATA_DIR}/organisation_isLocatedIn_place_header.csv,${LDBC_SNB_DATA_DIR}/organisation_isLocatedIn_place${LDBC_SNB_DATA_POSTFIX}" \
  --delimiter '|'