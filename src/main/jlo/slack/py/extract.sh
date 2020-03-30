. ~/.bashrc
# ./run.sh reference.py
# ./run.sh extract_slack.py
archive=`ls data -tp | grep /$ | head -1`
archive=${archive%?}
echo "batch id is :${archive}"
./run.sh transform_load.py ${archive}
cd data
zip -r ausinnovation-slack-${archive}.zip ${archive}
cd ..
