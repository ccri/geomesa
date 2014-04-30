package geomesa.core.data.mapreduce

import geomesa.core.Acc15VersionSpecificOperations

/**
 * Created by davidm on 4/30/14.
 */
object FeatureIngestMapperWrapper extends AbstractFeatureIngestMapperWrapper(Acc15VersionSpecificOperations) {
  class FeatureIngestMapper extends AbstractFeatureIngestMapper
}