rm build/cqs.tsv 2>/dev/null
mvn dependency:tree -Dverbose > build/deps-raw
grep ':compile' build/deps-raw | grep -v 'omitted' | grep -v 'org.locationtech.geomesa' | sed -e 's/\[INFO\] //' -e 's/[\| +-]*//' -e 's/(.*)//' -e 's/ //g' -e 's/\(.*\):\(.*\):jar:\(.*\):\(\w*\)/\1:\2\t\3\t\4/' | sort | uniq > build/cqs.tsv
echo "" >> build/cqs.tsv
for cq in $(grep ':provided' build/deps-raw | grep '^\[INFO\] +-' | grep -v 'org.locationtech.geomesa' | sed -e 's/\[INFO\] +- //' -e 's/(.*)//' | sort | uniq); do
  dep="$(echo $cq | sed 's/\(.*\):\(.*\):jar:\(.*\):\(\w*\)/\1:\2\t\3\t\4/')"
  reg=$(echo "${dep%	*}" | sed 's/\s\s*/\\s*/g')
  if [ -z "$(grep $reg build/cqs.tsv)" ]; then
    echo $dep >> build/cqs.tsv
  fi
done
echo "" >> build/cqs.tsv
for cq in $(grep ':test' build/deps-raw | grep '^\[INFO\] +-' | grep -v 'org.locationtech.geomesa' | sed -e 's/\[INFO\] +- //' -e 's/(.*)//' | sort | uniq); do
  dep="$(echo $cq | sed 's/\(.*\):\(.*\):jar:\(.*\):\(\w*\)/\1:\2\t\3\t\4/')"
  reg=$(echo "${dep%	*}" | sed 's/\s\s*/\\s*/g')
  if [ -z "$(grep $reg build/cqs.tsv)" ]; then
    echo $dep >> build/cqs.tsv
  fi
done
rm build/deps-raw
