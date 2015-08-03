package com.linkedin.thirdeye.anomaly.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.database.FunctionTable;
import com.linkedin.thirdeye.anomaly.database.FunctionTableRow;
import com.linkedin.thirdeye.anomaly.handler.AnomalyResultHandlerDatabase;
import com.linkedin.thirdeye.anomaly.server.ThirdEyeServerQueryUtils;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeRange;

/**
 * Loads rules from the function table in the anomaly database.
 */
public class AnomalyDetectionTaskBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyDetectionTaskBuilder.class);

  private final List<AnomalyDetectionDriverConfig> collectionDrivers;
  private final AnomalyDatabaseConfig dbConfig;

  public AnomalyDetectionTaskBuilder(List<AnomalyDetectionDriverConfig> collectionDrivers,
      AnomalyDatabaseConfig dbConfig) {
    super();
    this.collectionDrivers = collectionDrivers;
    this.dbConfig = dbConfig;
  }

  /**
   * @param collectionsConfig
   * @param dbConfig
   * @param timeRange
   * @return
   *  A list of AnomalyDetectionTasks to execute
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws IOException
   * @throws SQLException
   */
  public List<AnomalyDetectionTask> buildTasks(TimeRange timeRange, AnomalyDetectionFunctionFactory functionFactory)
      throws InstantiationException, IllegalAccessException, IOException, SQLException {

    List<AnomalyDetectionTask> tasks = new LinkedList<AnomalyDetectionTask>();

    List<? extends FunctionTableRow> rows = FunctionTable.selectRows(dbConfig, functionFactory.getFunctionRowClass());

    for (FunctionTableRow functionTableRow : rows) {
      try {
        // find the collection
        AnomalyDetectionDriverConfig collectionDriverConfig = AnomalyDetectionDriverConfig.find(collectionDrivers,
            functionTableRow.getCollectionName());

        // load star tree
        StarTreeConfig starTreeConfig = ThirdEyeServerQueryUtils.getStarTreeConfig(collectionDriverConfig);

        // load the function
        AnomalyDetectionFunction function = functionFactory.getFunction(starTreeConfig, dbConfig, functionTableRow);

        // create task info
        AnomalyDetectionTaskInfo taskInfo = new AnomalyDetectionTaskInfo(functionTableRow.getFunctionName(),
            functionTableRow.getFunctionId(), functionTableRow.getFunctionDescription(), timeRange);

        // make a handler
        AnomalyResultHandler resultHandler = new AnomalyResultHandlerDatabase(dbConfig);
        resultHandler.init(starTreeConfig, new HandlerProperties());

        // make the task
        AnomalyDetectionTask task = new AnomalyDetectionTask(starTreeConfig, collectionDriverConfig, taskInfo,
            function, resultHandler);

        tasks.add(task);
      } catch (Exception e) {
        LOGGER.error("could not create function for function_id={}", functionTableRow.getFunctionId(), e);
      }
    }

    return tasks;
  }
}
