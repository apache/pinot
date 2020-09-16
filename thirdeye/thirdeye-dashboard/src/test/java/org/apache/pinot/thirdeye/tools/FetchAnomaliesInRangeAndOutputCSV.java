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

import org.apache.pinot.thirdeye.datalayer.util.DaoProviderUtil;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FetchAnomaliesInRangeAndOutputCSV {
  private static final Logger LOG = LoggerFactory.getLogger(FetchAnomaliesInRangeAndOutputCSV.class);
  private static DateTime dataRangeStart;
  private static DateTime dataRangeEnd;
  private static DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm");

  public static void init(File persistenceFile) {
    DaoProviderUtil.init(persistenceFile);
  }

  public static void outputResultNodesToFile(File outputFile,
      List<FetchMetricDataAndExistingAnomaliesTool.ResultNode> resultNodes){
    try{
      BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));

      int rowCount = 0;
      if(resultNodes.size() > 0) {
        bw.write(StringUtils.join(resultNodes.get(0).getSchema(), ","));
        bw.newLine();
        for (FetchMetricDataAndExistingAnomaliesTool.ResultNode n : resultNodes) {
          bw.write(n.toString());
          bw.newLine();
          rowCount++;
        }
        LOG.info("{} anomaly results has been written...", rowCount);
      }
      bw.close();
    }
    catch (IOException e){
      LOG.error("Unable to write date-dimension anomaly results to given file {}", e);
    }
  }

  /**
   * Ouput merged anomaly results for given metric and time range
   * @param args List of arguments
   *             0: path to persistence file
   *             1: collection name
   *             2: metric name
   *             3: monitoring start time in ISO format
   *             4: timezone code
   *             5: monitoring length in days
   *             6: Output path
   */
  public static void main(String args[]){
    if(args.length < 7){
      LOG.error("Insufficient number of arguments");
      return;
    }

    String persistencePath = args[0];
    String collection = args[1];
    String metric = args[2];
    String monitoringDateTime = args[3];
    DateTimeZone dateTimeZone = DateTimeZone.forID(args[4]);
    int monitoringLength = Integer.valueOf(args[5]);
    File output_folder = new File(args[6]);

    FetchMetricDataAndExistingAnomaliesTool thirdEyeDAO = null;
    try {
      thirdEyeDAO = new FetchMetricDataAndExistingAnomaliesTool(new File(persistencePath));
    }
    catch (Exception e){
      LOG.error("Error in loading the persistence file: {}", e);
      return;
    }

    DateTime monitoringWindowStartTime = ISODateTimeFormat.dateTimeParser().parseDateTime(monitoringDateTime).withZone(dateTimeZone);
    Period period = new Period(0, 0, 0, monitoringLength, 0, 0, 0, 0);
    dataRangeStart = monitoringWindowStartTime.minus(period); // inclusive start
    dataRangeEnd = monitoringWindowStartTime; // exclusive end

    if(!output_folder.exists() || !output_folder.canWrite()){
      LOG.error("{} is not accessible", output_folder.getAbsoluteFile());
      return;
    }


    // Print Merged Results
    List<FetchMetricDataAndExistingAnomaliesTool.ResultNode> resultNodes = thirdEyeDAO.fetchMergedAnomaliesInRange(
        collection, metric, dataRangeStart, dataRangeEnd);

    LOG.info("Printing merged anomaly results from db...");
    String outputname = output_folder.getAbsolutePath() + "/" +
        "merged_" + metric + "_" + dateTimeFormatter.print(dataRangeStart) +
        "_" + dateTimeFormatter.print(dataRangeEnd) + ".csv";
    outputResultNodesToFile(new File(outputname), resultNodes);
    LOG.info("Finish job and print merged anomaly results from db in {}...", outputname);
  }

}
