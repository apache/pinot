/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.datasource.pinot;

import org.apache.pinot.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.dashboard.Utils;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.pinot.resultset.ThirdEyeResultSetGroup;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains methods to return max date time for datasets in pinot
 */
public class PinotDataSourceTimeQuery {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotDataSourceTimeQuery.class);
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private final static String TIME_QUERY_TEMPLATE = "SELECT %s(%s) FROM %s WHERE %s";

  private final PinotThirdEyeDataSource pinotThirdEyeDataSource;

  public PinotDataSourceTimeQuery(PinotThirdEyeDataSource pinotThirdEyeDataSource) {
    this.pinotThirdEyeDataSource = pinotThirdEyeDataSource;
  }

  /**
   * Returns the max time in millis for dataset in pinot
   * @param dataset
   * @return max date time in millis
   */
  public long getMaxDateTime(String dataset) {
    long maxTime = queryTimeSpecFromPinot(dataset, "max");
    if (maxTime <= 0) {
      maxTime = System.currentTimeMillis();
    }
    return maxTime;
  }

  /**
   * Returns the earliest time in millis for a dataset in pinot
   * @param dataset name of the dataset
   * @return min (earliest) date time in millis. Returns 0 if dataset is not found
   */
  public long getMinDateTime(String dataset) {
    return queryTimeSpecFromPinot(dataset, "min");
  }

  private long queryTimeSpecFromPinot(final String dataset, final String functionName) {
    long maxTime = 0;
    try {
      DatasetConfigDTO datasetConfig = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(dataset);
      // By default, query only offline, unless dataset has been marked as realtime
      TimeSpec timeSpec = ThirdEyeUtils.getTimestampTimeSpecFromDatasetConfig(datasetConfig);

      long cutoffTime = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1);
      String timeClause = SqlUtils
          .getBetweenClause(new DateTime(0, DateTimeZone.UTC), new DateTime(cutoffTime, DateTimeZone.UTC), timeSpec, dataset);

      String maxTimeSql = SqlUtils.escapeSqlReservedKeywords(
          String.format(TIME_QUERY_TEMPLATE, functionName, timeSpec.getColumnName(), dataset, timeClause));
      PinotQuery maxTimePinotQuery = new PinotQuery(maxTimeSql, dataset);

      ThirdEyeResultSetGroup resultSetGroup;
      final long tStart = System.nanoTime();
      try {
        pinotThirdEyeDataSource.refreshSQL(maxTimePinotQuery);
        resultSetGroup = pinotThirdEyeDataSource.executeSQL(maxTimePinotQuery);
        ThirdeyeMetricsUtil
            .getRequestLog().success(this.pinotThirdEyeDataSource.getName(), dataset, timeSpec.getColumnName(), tStart, System.nanoTime());
      } catch (ExecutionException e) {
        ThirdeyeMetricsUtil.getRequestLog().failure(this.pinotThirdEyeDataSource.getName(), dataset, timeSpec.getColumnName(), tStart, System.nanoTime(), e);
        throw e;
      }

      if (resultSetGroup.size() == 0 || resultSetGroup.get(0).getRowCount() == 0) {
        LOGGER.error("Failed to get latest max time for dataset {} with SQL: {}", dataset, maxTimePinotQuery.getQuery());
      } else {
        DateTimeZone timeZone = Utils.getDataTimeZone(dataset);

        long endTime = new Double(resultSetGroup.get(0).getDouble(0)).longValue();
        // endTime + 1 to make sure we cover the time range of that time value.
        String timeFormat = timeSpec.getFormat();
        if (StringUtils.isBlank(timeFormat) || TimeSpec.SINCE_EPOCH_FORMAT.equals(timeFormat)) {
          maxTime = timeSpec.getDataGranularity().toMillis(endTime + 1, timeZone) - 1;
        } else {
          DateTimeFormatter inputDataDateTimeFormatter =
              DateTimeFormat.forPattern(timeFormat).withZone(timeZone);
          DateTime endDateTime = DateTime.parse(String.valueOf(endTime), inputDataDateTimeFormatter);
          Period oneBucket = datasetConfig.bucketTimeGranularity().toPeriod();
          maxTime = endDateTime.plus(oneBucket).getMillis() - 1;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Exception getting maxTime from collection: {}", dataset, e);
    }
    return maxTime;
  }
}
