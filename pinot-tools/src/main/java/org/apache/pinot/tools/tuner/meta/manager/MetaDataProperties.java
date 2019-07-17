package org.apache.pinot.tools.tuner.meta.manager;

import org.apache.commons.math.fraction.BigFraction;


/*
 * Access fields of metadata.properties
 */
public interface MetaDataProperties {
  public String WEIGHTED_SUM_CARDINALITY = "w_cardinality";
  public String SUM_DOCS = "n_totalDocs";
  public String NUM_SEGMENTS_HAS_INVERTED_INDEX = "n_has_inverted_index";
  public String NUM_SEGMENTS_SORTED = "n_isSorted";
  public String NUM_SEGMENTS_COUNT = "n_segments_count";

  String getColField(String tableNameWithoutType, String columnName,
      String fieldName); //get aggregated (sum and weighted sum) of metadata

  String getSegmentField(String tableNameWithoutType, String columnName, String segmentName,
      String fieldName); //get metadata of individual segment

  BigFraction getAverageCardinality(String tableNameWithoutType, String columnName);
  boolean hasInvertedIndex(String tableNameWithoutType, String columnName);
}
