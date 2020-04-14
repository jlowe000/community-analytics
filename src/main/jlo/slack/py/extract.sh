. ~/.bashrc
# ./run.sh reference.py
./run.sh extract_slack.py
current_version=v2
archive=`ls data -tp | grep /$ | head -1`
archive=${archive%?}
echo "batch id is :${archive}"
./run.sh transform_load.py ${archive}
cd data
zip -r ausinnovation-slack-${archive}-data-${current_version}.zip ${archive}/api ${archive}/files ${archive}/json ${archive}/metadata
zip -r ausinnovation-slack-${archive}-metrics-${current_version}.zip ${archive}/csv ${archive}/metrics ${archive}/metadata
cd ..
./run.sh transform_load.py
cd data
zip -r ausinnovation-slack-${archive}-master-metrics-${current_version}.zip master metrics
cd ..
