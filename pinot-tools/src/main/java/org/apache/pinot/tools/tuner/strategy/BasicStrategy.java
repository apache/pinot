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
      Map<String, Map<String, BasicMergerObj>> MapperOut);
  //Accumulate the stats of a query to /table/column/BasicMergerObj

  void merger(BasicMergerObj basicMergerObj, BasicMergerObj basicMergerObjToMerge);
  //Merge two BasicMergerObj from same /table/column

  void reporter(String tableNameWithoutType, Map<String, BasicMergerObj> mergedOut);
  //print/email results
}
