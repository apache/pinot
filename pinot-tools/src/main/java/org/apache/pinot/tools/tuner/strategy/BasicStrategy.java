package org.apache.pinot.tools.tuner.strategy;

import java.util.Map;
import org.apache.pinot.tools.tuner.meta.manager.MetaDataProperties;
import org.apache.pinot.tools.tuner.query.src.BasicQueryStats;


/**
 * Recommendation strategy
 */
public interface BasicStrategy {
  /**
   * Filter out irrelevant query stats to target a specific table or specific range of nESI
   * @param queryStats the stats extracted and parsed from QuerySrc
   * @return
   */
  boolean filter(BasicQueryStats queryStats);

  /**
   * Accumulate the parsed queryStats to corresponding entry in MapperOut, see FrequencyImpl for ex
   * @param queryStats input, the stats extracted and parsed from QuerySrc
   * @param metaDataProperties input, the metaDataProperties where cardinality info can be get from
   * @param MapperOut output, map of /tableMame: String/columnName: String/BasicMergerObj
   */
  void accumulator(BasicQueryStats queryStats, MetaDataProperties metaDataProperties,
      Map<String, Map<String, BasicMergerObj>> MapperOut);

  /**
   * merge two BasicMergerObj with same /tableName/colName
   * @param basicMergerObj input
   * @param basicMergerObjToMerge input
   */
  void merger(BasicMergerObj basicMergerObj, BasicMergerObj basicMergerObjToMerge);
  //Merge two BasicMergerObj from same /table/column

  /**
   * Generate a report for recommendation using mergedOut:/colName/BasicMergerObj
   * @param tableNameWithoutType input
   * @param mergedOut input
   */
  void reporter(String tableNameWithoutType, Map<String, BasicMergerObj> mergedOut);
  //print/email results
}
