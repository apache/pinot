package org.apache.pinot.tools.tuner.strategy;

import java.util.Map;
import org.apache.pinot.tools.tuner.meta.manager.MetaDataProperties;
import org.apache.pinot.tools.tuner.query.src.BasicQueryStats;


public interface BasicStrategy {
  boolean filter(BasicQueryStats queryStats);

  void accumulator(BasicQueryStats queryStats, MetaDataProperties metaDataProperties, Map<String, Map<String, ColumnStatsObj>> MapperOut);

  void merger(ColumnStatsObj columnStatsObj, ColumnStatsObj columnStatsObjToMerge);

  void reporter(String tableNameWithType, Map<String, ColumnStatsObj> MergedOut);
}
