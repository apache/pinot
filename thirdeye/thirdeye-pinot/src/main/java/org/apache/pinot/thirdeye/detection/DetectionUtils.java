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

package org.apache.pinot.thirdeye.detection;

import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.api.DimensionMap;
import org.apache.pinot.thirdeye.dataframe.BooleanSeries;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.LongSeries;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.MetricConfigBean;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.spi.components.BaseComponent;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.apache.pinot.thirdeye.detection.spi.model.EventSlice;
import org.apache.pinot.thirdeye.detection.spi.model.InputData;
import org.apache.pinot.thirdeye.detection.spi.model.InputDataSpec;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


public class DetectionUtils {
  // TODO anomaly should support multimap
  public static DimensionMap toFilterMap(Multimap<String, String> filters) {
    DimensionMap map = new DimensionMap();
    for (Map.Entry<String, String> entry : filters.entries()) {
      map.put(entry.getKey(), entry.getValue());
    }
    return map;
  }

  // Check if a string is a component reference
  public static boolean isReferenceName(String key) {
    return key.startsWith("$");
  }

  // get the component name from the reference key
  // example "$myRule:ALGORITHM:0" -> "myRule:ALGORITHM:0"
  public static String getComponentName(String key) {
    if (isReferenceName(key)) return key.substring(1);
    else throw new IllegalArgumentException("not a component reference key. should starts with $");
  }

  // get the spec class name for a component class
  public static String getSpecClassName(Class<BaseComponent> componentClass) {
    ParameterizedType genericSuperclass = (ParameterizedType) componentClass.getGenericInterfaces()[0];
    return (genericSuperclass.getActualTypeArguments()[0].getTypeName());
  }


  /**
   * Helper for creating a list of anomalies from a boolean series.
   *
   * @param slice metric slice
   * @param df time series with COL_TIME and at least one boolean value series
   * @param seriesName name of the value series
   * @param endTime end time of this detection window
   * @param dataset dataset config for the metric
   * @return list of anomalies
   */
  public static List<MergedAnomalyResultDTO> makeAnomalies(MetricSlice slice, DataFrame df, String seriesName, long endTime, DatasetConfigDTO dataset) {
    if (df.isEmpty()) {
      return Collections.emptyList();
    }

    df = df.filter(df.getLongs(COL_TIME).between(slice.getStart(), slice.getEnd())).dropNull(COL_TIME);

    if (df.isEmpty()) {
      return Collections.emptyList();
    }

    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    LongSeries sTime = df.getLongs(COL_TIME);
    BooleanSeries sVal = df.getBooleans(seriesName);

    int lastStart = -1;
    for (int i = 0; i < df.size(); i++) {
      if (sVal.isNull(i) || !BooleanSeries.booleanValueOf(sVal.get(i))) {
        // end of a run
        if (lastStart >= 0) {
          long start = sTime.get(lastStart);
          long end = sTime.get(i);
          anomalies.add(makeAnomaly(slice.withStart(start).withEnd(end)));
        }
        lastStart = -1;

      } else {
        // start of a run
        if (lastStart < 0) {
          lastStart = i;
        }
      }
    }

    // end of current run
    if (lastStart >= 0) {
      long start = sTime.get(lastStart);
      long end = start + 1;

      // guess-timate of next time series timestamp
      if (dataset != null) {
        Period period = dataset.bucketTimeGranularity().toPeriod();
        DateTimeZone timezone = DateTimeZone.forID(dataset.getTimezone());

        long lastTimestamp = sTime.getLong(sTime.size() - 1);

        end = new DateTime(lastTimestamp, timezone).plus(period).getMillis();
      }

      // truncate at analysis end time
      end = Math.min(end, endTime);

      anomalies.add(makeAnomaly(slice.withStart(start).withEnd(end)));
    }

    return anomalies;
  }

  /**
   * Helper for creating an anomaly for a given metric slice. Injects properties such as
   * metric name, filter dimensions, etc.
   *
   * @param slice metric slice
   * @return anomaly template
   */
  public static MergedAnomalyResultDTO makeAnomaly(MetricSlice slice) {
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setStartTime(slice.getStart());
    anomaly.setEndTime(slice.getEnd());

    return anomaly;
  }

  /**
   * Helper for consolidate last time stamps in all nested detection pipelines
   * @param nestedLastTimeStamps all nested last time stamps
   * @return the last time stamp
   */
  public static long consolidateNestedLastTimeStamps(Collection<Long> nestedLastTimeStamps){
    if(nestedLastTimeStamps.isEmpty() || nestedLastTimeStamps.contains(-1L)){
      return -1L;
    }
    return Collections.min(nestedLastTimeStamps);
  }
}
