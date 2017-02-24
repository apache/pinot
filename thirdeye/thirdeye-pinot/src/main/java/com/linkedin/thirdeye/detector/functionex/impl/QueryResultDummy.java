package com.linkedin.thirdeye.detector.functionex.impl;

import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionEx;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExResult;
import com.linkedin.thirdeye.detector.functionex.dataframe.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QueryResultDummy extends AnomalyFunctionEx {
  private static final Logger LOG = LoggerFactory.getLogger(QueryResultDummy.class);

  @Override
  public AnomalyFunctionExResult apply() throws Exception {
    String datasource = getConfig("datasource");
    String config = getConfig("query");

    LOG.info("Querying '{}' with '{}'", datasource, config);
    DataFrame df = queryDataSource(datasource, config);

    LOG.info("Got query result with {} columns and {} rows", df.getSeriesNames().size(), df.getIndex().size());
    LOG.info(df.toString());

    return new AnomalyFunctionExResult();
  }
}
