. ~/.bashrc
# ./run.sh reference.py
./run.sh extract_slack.py
current_version=v2
archive=`ls data -tp | grep /$ | head -1`
archive=${archive%?}
echo "batch id is :${archive}"
./run.sh transform_load.py ${archive}
cd data
zip -r ausinnovation-slack-${archive}-${current_version}.zip ${archive}
# rm current-dataset
# ln -s ${archive} current-dataset
cd ..
