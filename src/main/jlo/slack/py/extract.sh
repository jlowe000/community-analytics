. ~/.bashrc
. ./env.sh
./run.sh scripts/extract_slack.py
current_version=v3
archive=`ls ${SLACK_DATA_HOME} -tp | grep /$ | head -1`
archive=${archive%?}
echo "batch id is :${archive}"
./run.sh scripts/transform_load.py ${archive}
rm ${SLACK_DATA_HOME}/metrics/csv/*
./run.sh scripts/transform_load.py
cd ${SLACK_DATA_HOME}
zip -r ausinnovation-slack-${archive}-data-${current_version}.zip ${archive}/api ${archive}/files ${archive}/json ${archive}/metadata
zip -r ausinnovation-slack-${archive}-metrics-${current_version}.zip ${archive}/csv ${archive}/metrics ${archive}/metadata
zip -r ausinnovation-slack-${archive}-master-metrics-${current_version}.zip master/csv/*_data.csv metrics
cd -
./run.sh scripts/load_pg.py
./run.sh scripts/load_oracle.py
