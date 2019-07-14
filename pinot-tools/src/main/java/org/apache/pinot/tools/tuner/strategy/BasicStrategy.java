package org.apache.pinot.tools.tuner.strategy;

import java.util.Map;
import org.apache.pinot.tools.tuner.meta.manager.MetaDataProperties;
import org.apache.pinot.tools.tuner.query.src.BasicQueryStats;

/*
 * Recommendation strategy
 */
public interface BasicStrategy {
  boolean filter(BasicQueryStats queryStats);//Filter out some irrelevant tables.

  void accumulator(BasicQueryStats queryStats, MetaDataProperties metaDataProperties, Map<String, Map<String, ColumnStatsObj>> MapperOut);
  //Accumulate the stats of a query to /table/column/ColumnStatsObj

  void merger(ColumnStatsObj columnStatsObj, ColumnStatsObj columnStatsObjToMerge);
  //Merge two ColumnStatsObj from same /table/column

  void reporter(String tableNameWithType, Map<String, ColumnStatsObj> MergedOut);
  //print/email results
}
