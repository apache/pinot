package org.apache.pinot.tools.tuner.meta.manager;

import org.apache.commons.math.fraction.BigFraction;


public interface MetaDataProperties {
  String WEIGHTED_SUM_CARDINALITY = "w_cardinality";
  String SUM_DOCS = "n_totalDocs";

  String getColField(String tableNameWithType, String columnName, String fieldName); //get aggregated (sum and weighted sum) of metadata
  String getSegmentField(String tableNameWithType, String columnName, String segmentName, String fieldName); //get metadata of individual segment
  BigFraction getAverageCardinality(String tableNameWithType, String columnName);
}
