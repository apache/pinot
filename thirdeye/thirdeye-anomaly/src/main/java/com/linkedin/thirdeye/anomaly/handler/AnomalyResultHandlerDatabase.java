package com.linkedin.thirdeye.anomaly.handler;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionTaskInfo;
import com.linkedin.thirdeye.anomaly.api.AnomalyResultHandler;
import com.linkedin.thirdeye.anomaly.api.HandlerProperties;
import com.linkedin.thirdeye.anomaly.api.external.AnomalyResult;
import com.linkedin.thirdeye.anomaly.database.AnomalyTable;
import com.linkedin.thirdeye.anomaly.database.AnomalyTableRow;
import com.linkedin.thirdeye.anomaly.util.DimensionKeyUtils;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.StarTreeConfig;

/**
 *
 */
public class AnomalyResultHandlerDatabase implements AnomalyResultHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyResultHandlerDatabase.class);

  private final AnomalyDatabaseConfig dbConfig;

  private final AnomalyResultHandlerLogger loggerHandler = new AnomalyResultHandlerLogger();

  private StarTreeConfig starTreeConfig;

  public AnomalyResultHandlerDatabase(AnomalyDatabaseConfig dbConfig) throws IOException {
    super();
    this.dbConfig = dbConfig;
    if (dbConfig.isCreateTablesIfNotExists()) {
     AnomalyTable.createTable(dbConfig);
    }
  }

  @Override
  public void init(StarTreeConfig starTreeConfig, HandlerProperties handlerConfig) {
    loggerHandler.init(starTreeConfig, handlerConfig);
    this.starTreeConfig = starTreeConfig;
  }

  /**
   * If the result is anomalous, insert a new row in the anomaly table.
   */
  @Override
  public void handle(AnomalyDetectionTaskInfo taskInfo, DimensionKey dimensionKey, double dimensionKeyContribution,
      Set<String> metrics, AnomalyResult result) throws IOException {
    if (result.isAnomaly() == false) {
      return;
    }

    AnomalyTableRow row = new AnomalyTableRow();
    row.setFunctionTable(dbConfig.getFunctionTableName());
    row.setFunctionId(taskInfo.getFunctionId());
    row.setFunctionDescription(taskInfo.getFunctionDescription());
    row.setFunctionName(taskInfo.getFunctionName());
    row.setCollection(starTreeConfig.getCollection());
    row.setTimeWindow(result.getTimeWindow());
    row.setNonStarCount(dimensionKey.getDimensionValues().length - DimensionKeyUtils.getStarCount(dimensionKey));
    row.setDimensions(DimensionKeyUtils.toJsonString(starTreeConfig.getDimensions(), dimensionKey));
    row.setDimensionsContribution(dimensionKeyContribution);
    row.setMetrics(metrics);
    row.setAnomalyScore(result.getAnomalyScore());
    row.setAnomalyVolume(result.getAnomalyVolume());
    row.setProperties(result.getProperties());

    AnomalyTable.insertRow(dbConfig, row);

    loggerHandler.handle(taskInfo, dimensionKey, dimensionKeyContribution, metrics, result);
  }

}
