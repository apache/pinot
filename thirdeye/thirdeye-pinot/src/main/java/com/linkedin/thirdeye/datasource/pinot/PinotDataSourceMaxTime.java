/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.datasource.pinot;

import com.linkedin.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.pinot.resultset.ThirdEyeResultSetGroup;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains methods to return max date time for datasets in pinot
 */
public class PinotDataSourceMaxTime {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotDataSourceMaxTime.class);
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private final static String COLLECTION_MAX_TIME_QUERY_TEMPLATE = "SELECT max(%s) FROM %s WHERE %s >= %s";

  private final Map<String, Long> collectionToPrevMaxDataTimeMap = new ConcurrentHashMap<String, Long>();
  private final PinotThirdEyeDataSource pinotThirdEyeDataSource;

  public PinotDataSourceMaxTime(PinotThirdEyeDataSource pinotThirdEyeDataSource) {
    this.pinotThirdEyeDataSource = pinotThirdEyeDataSource;
  }

  /**
   * Returns the max time in millis for dataset in pinot
   * @param dataset
   * @return max date time in millis
   */
  public long getMaxDateTime(String dataset) {
    LOGGER.debug("Loading maxDataTime cache {}", dataset);
    long maxTime = 0;
    try {
      DatasetConfigDTO datasetConfig = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(dataset);
      // By default, query only offline, unless dataset has been marked as realtime
      String tableName = ThirdEyeUtils.computeTableName(dataset);
      TimeSpec timeSpec = ThirdEyeUtils.getTimestampTimeSpecFromDatasetConfig(datasetConfig);
      long prevMaxDataTime = getPrevMaxDataTime(dataset);
      String maxTimePql = String.format(COLLECTION_MAX_TIME_QUERY_TEMPLATE, timeSpec.getColumnName(), tableName,
          timeSpec.getColumnName(), prevMaxDataTime);
      PinotQuery maxTimePinotQuery = new PinotQuery(maxTimePql, tableName);

      ThirdEyeResultSetGroup resultSetGroup;
      final long tStart = System.nanoTime();
      try {
        pinotThirdEyeDataSource.refreshPQL(maxTimePinotQuery);
        resultSetGroup = pinotThirdEyeDataSource.executePQL(maxTimePinotQuery);
        ThirdeyeMetricsUtil.getRequestLog().success(this.pinotThirdEyeDataSource.getName(), dataset, timeSpec.getColumnName(), tStart, System.nanoTime());
      } catch (ExecutionException e) {
        ThirdeyeMetricsUtil.getRequestLog().failure(this.pinotThirdEyeDataSource.getName(), dataset, timeSpec.getColumnName(), tStart, System.nanoTime(), e);
        throw e;
      }

      if (resultSetGroup.size() == 0 || resultSetGroup.get(0).getRowCount() == 0) {
        LOGGER.error("Failed to get latest max time for dataset {} with PQL: {}", tableName, maxTimePinotQuery.getPql());
        this.collectionToPrevMaxDataTimeMap.remove(dataset);
      } else {
        long endTime = new Double(resultSetGroup.get(0).getDouble(0)).longValue();
        this.collectionToPrevMaxDataTimeMap.put(dataset, endTime);
        // endTime + 1 to make sure we cover the time range of that time value.
        String timeFormat = timeSpec.getFormat();
        if (StringUtils.isBlank(timeFormat) || TimeSpec.SINCE_EPOCH_FORMAT.equals(timeFormat)) {
          maxTime = timeSpec.getDataGranularity().toMillis(endTime + 1) - 1;
        } else {
          DateTimeFormatter inputDataDateTimeFormatter =
              DateTimeFormat.forPattern(timeFormat).withZone(Utils.getDataTimeZone(dataset));
          DateTime endDateTime = DateTime.parse(String.valueOf(endTime), inputDataDateTimeFormatter);
          Period oneBucket = datasetConfig.bucketTimeGranularity().toPeriod();
          maxTime = endDateTime.plus(oneBucket).getMillis() - 1;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Exception getting maxTime from collection: {}", dataset, e);
      this.collectionToPrevMaxDataTimeMap.remove(dataset);
    }
    if (maxTime <= 0) {
      maxTime = System.currentTimeMillis();
    }
    return maxTime;
  }

  private long getPrevMaxDataTime(String collection) {
    if (this.collectionToPrevMaxDataTimeMap.containsKey(collection)) {
      return collectionToPrevMaxDataTimeMap.get(collection);
    }
    return 0;
  }
}
