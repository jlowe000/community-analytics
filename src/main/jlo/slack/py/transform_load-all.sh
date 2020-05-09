. ~/.bashrc
. ./env.sh
current_version=v3
rm ${SLACK_DATA_HOME}/master/csv/*
rm ${SLACK_DATA_HOME}/metrics/csv/*
for archive in `ls data -tpr | grep 2020.*/$`; do
  archive=${archive%?}
  echo "batch id is :${archive}"
  rm ${SLACK_DATA_HOME}/${archive}/csv/*
  rm ${SLACK_DATA_HOME}/${archive}/metrics/*
  rm ${SLACK_DATA_HOME}/${archive}/json/*
  ./run.sh scripts/transform_load.py ${archive}
  cd ${SLACK_DATA_HOME}
  zip -r ausinnovation-slack-${archive}-data-${current_version}.zip ${archive}/api ${archive}/files ${archive}/json ${archive}/metadata
  zip -r ausinnovation-slack-${archive}-metrics-${current_version}.zip ${archive}/csv ${archive}/metrics ${archive}/metadata
  cd -
done
./run.sh scripts/transform_load.py
cd ${SLACK_DATA_HOME}
zip -r ausinnovation-slack-${archive}-master-metrics-${current_version}.zip master/csv/*data.csv metrics
cd -
