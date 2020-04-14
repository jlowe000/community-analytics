. ~/.bashrc
# ./run.sh reference.py
# ./run.sh extract_slack.py
current_version=v3
rm data/master/csv/*
rm data/metrics/csv/*
for archive in `ls data -tpr | grep 2020.*/$`; do
  archive=${archive%?}
  echo "batch id is :${archive}"
  rm data/${archive}/csv/*
  rm data/${archive}/metrics/*
  rm data/${archive}/json/*
  ./run.sh transform_load.py ${archive}
  cd data
  zip -r ausinnovation-slack-${archive}-data-${current_version}.zip ${archive}/api ${archive}/files ${archive}/json ${archive}/metadata
  zip -r ausinnovation-slack-${archive}-metrics-${current_version}.zip ${archive}/csv ${archive}/metrics ${archive}/metadata
  cd ..
done
./run.sh transform_load.py
cd data
zip -r ausinnovation-slack-${archive}-master-metrics-${current_version}.zip master metrics
cd ..

