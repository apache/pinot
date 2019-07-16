package org.apache.pinot.tools.tuner.strategy;

import java.util.Map;
import org.apache.pinot.tools.tuner.meta.manager.MetaDataProperties;
import org.apache.pinot.tools.tuner.query.src.BasicQueryStats;


/*
 * Recommendation strategy
 */
public interface BasicStrategy {
  boolean filter(BasicQueryStats queryStats);
  //Filter out some irrelevant tables.

  void accumulator(BasicQueryStats queryStats, MetaDataProperties metaDataProperties,
      Map<String, Map<String, MergerObj>> MapperOut);
  //Accumulate the stats of a query to /table/column/MergerObj

  void merger(MergerObj mergerObj, MergerObj mergerObjToMerge);
  //Merge two MergerObj from same /table/column

  void reporter(String tableNameWithoutType, Map<String, MergerObj> mergedOut);
  //print/email results
}
