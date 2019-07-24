package org.apache.pinot.tools.tuner.meta.manager;

import org.apache.commons.math.fraction.BigFraction;


/**
 * Load and access fields of metadata.properties
 */
public interface MetaManager {
  String WEIGHTED_SUM_CARDINALITY = "w_cardinality";
  String SUM_DOCS = "n_totalDocs";
  String NUM_SEGMENTS_HAS_INVERTED_INDEX = "n_has_inverted_index";
  String NUM_SEGMENTS_SORTED = "n_isSorted";
  String NUM_SEGMENTS_COUNT = "n_segments_count";

  /**
   * Get aggregated (sum and weighted sum) of metadata
   * @param tableNameWithoutType
   * @param columnName
   * @param fieldName one of {WEIGHTED_SUM_CARDINALITY, SUM_DOCS, NUM_SEGMENTS_HAS_INVERTED_INDEX, NUM_SEGMENTS_SORTED, NUM_SEGMENTS_COUNT}
   * @return
   */
  String getColField(String tableNameWithoutType, String columnName, String fieldName);

  /**
   * Getter to certain fields for a specific column, for forward compatibility precise inverted index prediction.
   * @param tableNameWithoutType
   * @param columnName
   * @param segmentName
   * @param fieldName
   * @return
   */
  String getSegmentField(String tableNameWithoutType, String columnName, String segmentName, String fieldName); //get metadata of individual segment

  /**
   * Get the selectivity calculated by weighted average of cardinality.
   * @param tableNameWithoutType
   * @param columnName
   * @return Selectivity, a BigFraction.
   */
  BigFraction getColumnSelectivity(String tableNameWithoutType, String columnName);

  boolean hasInvertedIndex(String tableNameWithoutType, String columnName);
}
