. ~/.bashrc
# ./run.sh reference.py
# ./run.sh extract_slack.py
backup_version=v1
current_version=v2
for archive in `ls data -tp | grep /$`; do
  archive=${archive%?}
  echo "batch id is :${archive}"
  rm data/${archive}/csv/*
  rm data/${archive}/json/*
  ./run.sh transform_load.py ${archive}
  cd data
  zip -r ausinnovation-slack-${archive}-${current_version}.zip ${archive}
  cd ..
done

