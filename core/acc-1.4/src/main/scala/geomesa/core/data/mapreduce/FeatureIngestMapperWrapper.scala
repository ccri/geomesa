package geomesa.core.data.mapreduce

import geomesa.core.Acc14VersionSpecificOperations

/**
 * Created by davidm on 4/30/14.
 */
object FeatureIngestMapperWrapper extends AbstractFeatureIngestMapperWrapper(Acc14VersionSpecificOperations) {
  class FeatureIngestMapper extends AbstractFeatureIngestMapper
}