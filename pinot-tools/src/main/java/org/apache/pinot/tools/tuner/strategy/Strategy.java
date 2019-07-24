package org.apache.pinot.tools.tuner.strategy;

import java.util.Map;
import org.apache.pinot.tools.tuner.meta.manager.MetaManager;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;


/**
 * Recommendation strategy
 */
public interface Strategy {

  /**
   * Filter out irrelevant query stats to target a specific table or specific range of nESI
   * @param queryStats the stats extracted and parsed from QuerySrc
   * @return
   */
  boolean filter(AbstractQueryStats queryStats);

  /**
   * Accumulate the parsed queryStats to corresponding entry in MapperOut, see FrequencyImpl for ex
   * @param queryStats input, the stats extracted and parsed from QuerySrc
   * @param metaManager input, the metaManager where cardinality info can be get from
   * @param AccumulatorOut output, map of /tableMame: String/columnName: String/AbstractMergerObj
   */
  void accumulate(AbstractQueryStats queryStats, MetaManager metaManager,
      Map<String, Map<String, AbstractAccumulator>> AccumulatorOut);

  /**
   * merge two AbstractMergerObj with same /tableName/colName
   * @param abstractAccumulator input
   * @param abstractAccumulatorToMerge input
   */
  void merge(AbstractAccumulator abstractAccumulator, AbstractAccumulator abstractAccumulatorToMerge);
  //Merge two AbstractMergerObj from same /table/column

  /**
   * Generate a report for recommendation using mergedOut:/colName/AbstractMergerObj
   * @param tableNameWithoutType input
   * @param mergedOut input
   */
  void report(String tableNameWithoutType, Map<String, AbstractAccumulator> mergedOut);
  //print/email results
}
