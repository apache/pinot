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

package org.apache.pinot.thirdeye.tools;

import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FetchMetricDataInRangeAndOutputCSV {
  private static final Logger LOG = LoggerFactory.getLogger(FetchAnomaliesInRangeAndOutputCSV.class);
  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_PORT = "1426";
  private static final String AUTHENTICATION_TOKEN = "";

  /**
   * Fetch metric historical data from server and parse the json object
   * @param args List of arguments
   *             0: Path to the persistence file
   *             1: dataset/collection name
   *             2: metric name
   *             3: retrieval start time in ISO format, e.g. 2016-01-01T12:00:00
   *             4: timezone code
   *             5: Duration
   *             6: Aggregation time granularity, DAYS, HOURS,
   *             7: dimensions drill down, ex: product,channel
   *             8: filter in json format, ex: {"channel":["guest-email","guest-sms"]}
   *             9: Output path
   */
  public static void main(String[] args) {
    if(args.length < 10){
      LOG.error("Error: Insufficient number of arguments", new IllegalArgumentException());
      return;
    }

    String path2PersistenceFile = args[0];
    String dataset = args[1];
    String metric = args[2];
    String monitoringStartTime = args[3];
    String timezone = args[4];
    int monitoringLength = Integer.valueOf(args[5]);
    String dimensions = args[7];
    String filters = args[8];

    DateTimeZone dateTimeZone = DateTimeZone.forID(timezone);
    DateTime monitoringWindowStartTime = ISODateTimeFormat.dateTimeParser()
        .parseDateTime(monitoringStartTime).withZone(dateTimeZone);
    String aggTimeGranularity = args[6];
    TimeUnit timeUnit = TimeUnit.valueOf(aggTimeGranularity);
    File output_folder = new File(args[9]);

    if(!output_folder.exists() || !output_folder.canWrite()){
      LOG.error("{} is not accessible", output_folder.getAbsoluteFile());
      return;
    }

    if(timeUnit == null){
      LOG.error("Illegal time granularity");
      return;
    }

    // Training data range
    Period period = null;
    switch (timeUnit) {
      case DAYS:
        period = new Period(0, 0, 0, Integer.valueOf(monitoringLength), 0, 0, 0, 0);
        break;
      case HOURS:
        period = new Period(0, 0, 0, 0, Integer.valueOf(monitoringLength), 0, 0, 0);
        break;
      case MINUTES:
        period = new Period(0, 0, 0, 0, 0, Integer.valueOf(monitoringLength), 0, 0);
        break;

    }
    DateTime dataRangeStart = monitoringWindowStartTime.minus(period); // inclusive start
    DateTime dataRangeEnd = monitoringWindowStartTime; // exclusive end
    DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm");



    String fname = output_folder.getAbsolutePath() +  "/" +
        metric + "_" + fmt.print(dataRangeStart) + "_" + fmt.print(dataRangeEnd) + ".csv";
    Map<String, Map<Long, String>> metricContent;
    try {
      FetchMetricDataAndExistingAnomaliesTool thirdEyeDAO =
          new FetchMetricDataAndExistingAnomaliesTool(new File(path2PersistenceFile));
      metricContent = thirdEyeDAO.fetchMetric(DEFAULT_HOST, Integer.valueOf(DEFAULT_PORT), AUTHENTICATION_TOKEN, dataset,
          metric, dataRangeStart, dataRangeEnd,
          timeUnit, dimensions,
          filters, dateTimeZone.getID());

      BufferedWriter bw = new BufferedWriter(new FileWriter(fname));

      List<String> keys = new ArrayList<>(metricContent.keySet());
      List<Long> dateTimes = new ArrayList<>(metricContent.get(keys.get(0)).keySet());
      Collections.sort(dateTimes);

      // Print Header
      for(String str: keys){
        bw.write("," + str);
      }
      bw.newLine();

      for(Long dt : dateTimes){
        bw.write(Long.toString(dt));
        for(String key : keys){
          Map<Long, String> map = metricContent.get(key);
          bw.write("," + map.get(dt));
        }
        bw.newLine();
      }
      bw.close();
    }
    catch (Exception e){
      LOG.error("Unable to access file {}", fname, e);
    }
  }
}
