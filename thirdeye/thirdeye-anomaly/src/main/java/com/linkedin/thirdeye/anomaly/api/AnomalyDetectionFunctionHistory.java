package com.linkedin.thirdeye.anomaly.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.linkedin.thirdeye.anomaly.api.external.AnomalyResult;
import com.linkedin.thirdeye.anomaly.database.AnomalyTable;
import com.linkedin.thirdeye.anomaly.database.AnomalyTableRow;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeRange;

/**
 *
 */
public class AnomalyDetectionFunctionHistory {

  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyDetectionFunctionHistory.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private AnomalyDatabaseConfig dbConfig;
  private StarTreeConfig starTreeConfig;
  private int functionId;

  private ListMultimap<DimensionKey, AnomalyResult> functionHistory;

  /**
   * @param dbConfig
   *  Database with anomaly table and function table
   * @param starTreeConfig
   * @param functionId
   *  Id of function in function table
   */
  public AnomalyDetectionFunctionHistory(StarTreeConfig starTreeConfig, AnomalyDatabaseConfig dbConfig, int functionId)
  {
    super();
    this.dbConfig = dbConfig;
    this.starTreeConfig = starTreeConfig;
    this.functionId = functionId;
  }

  /**
   * @param queryTimeRange
   *  Fetches history for this time range
   */
  public void init(TimeRange queryTimeRange) {
    functionHistory = ArrayListMultimap.create();
    try {
      List<AnomalyTableRow> anomalyTableRows = AnomalyTable.selectRows(dbConfig, functionId, queryTimeRange);
      for (AnomalyTableRow anomalyTableRow : anomalyTableRows) {
        try {
          DimensionKey dimensionKey = dimensionKeyFromJson(starTreeConfig.getDimensions(),
              anomalyTableRow.getDimensions());
          functionHistory.put(dimensionKey, new AnomalyResult(true, anomalyTableRow.getTimeWindow(),
              anomalyTableRow.getAnomalyScore(), anomalyTableRow.getAnomalyVolume(), anomalyTableRow.getProperties()));
        } catch (IOException e) {
          LOGGER.warn("malformed dimension key json string", e);
        }
      }
    } catch (SQLException e) {
      LOGGER.error("could not select anomaly history", e);
    }
  }

  /**
   * @param dimensionKey
   * @return
   *  A collection of all anomaly results produced by the function
   */
  public List<AnomalyResult> getHistoryForDimensionKey(DimensionKey dimensionKey) {
    return functionHistory.get(dimensionKey);
  }

  /**
   * @param dimensions
   * @return
   * @throws IOException
   */
  private static DimensionKey dimensionKeyFromJson(List<DimensionSpec> dimensionSpecs, String dimensionsString)
      throws IOException {
    ObjectReader reader = OBJECT_MAPPER.reader(Map.class);
    Map<String, String> dimensionsMap = reader.readValue(dimensionsString);
    String[] dimensionValues = new String[dimensionSpecs.size()];
    int idx = 0;
    for (DimensionSpec dimensionSpec : dimensionSpecs) {
      dimensionValues[idx] = dimensionsMap.get(dimensionSpec.getName());
      idx++;
    }

    return new DimensionKey(dimensionValues);
  }

}
