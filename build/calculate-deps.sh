rm deps-processed 2>/dev/null
mvn dependency:tree -Dverbose > deps-raw
grep ':compile' deps-raw | grep -v 'omitted' | grep -v 'org.locationtech.geomesa' | sed -e 's/\[INFO\] //' -e 's/[\| +-]*//' -e 's/(.*)//' -e 's/ //g' -e 's/\(.*\):\(.*\):jar:\(.*\):\(\w*\)/\1:\2\t\3\t\4/' | sort | uniq > deps-processed
echo "" >> deps-processed
for cq in $(grep ':provided' deps-raw | grep '^\[INFO\] +-' | grep -v 'org.locationtech.geomesa' | sed -e 's/\[INFO\] +- //' -e 's/(.*)//' | sort | uniq); do
  dep="$(echo $cq | sed 's/\(.*\):\(.*\):jar:\(.*\):\(\w*\)/\1:\2\t\3\t\4/')"
  reg=$(echo "${dep%	*}" | sed 's/\s\s*/\\s*/g')
  if [ -z "$(grep $reg deps-processed)" ]; then
    echo $dep >> deps-processed
  fi
done
echo "" >> deps-processed
for cq in $(grep ':test' deps-raw | grep '^\[INFO\] +-' | grep -v 'org.locationtech.geomesa' | sed -e 's/\[INFO\] +- //' -e 's/(.*)//' | sort | uniq); do
  dep="$(echo $cq | sed 's/\(.*\):\(.*\):jar:\(.*\):\(\w*\)/\1:\2\t\3\t\4/')"
  reg=$(echo "${dep%	*}" | sed 's/\s\s*/\\s*/g')
  if [ -z "$(grep $reg deps-processed)" ]; then
    echo $dep >> deps-processed
  fi
done
rm deps-raw
