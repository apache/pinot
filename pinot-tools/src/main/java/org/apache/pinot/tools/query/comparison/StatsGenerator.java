/**
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
package org.apache.pinot.tools.query.comparison;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StatsGenerator {
  private StatsGenerator() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(StatsGenerator.class);

  public static void generateReport(String dataFileName)
      throws IOException {
    List<DescriptiveStatistics> statisticsList = new ArrayList<>();

    String dataString;
    BufferedReader dataReader = new BufferedReader(new FileReader(dataFileName));

    // First line is treated as header
    String[] columns = dataReader.readLine().split("\\s+");
    int numColumns = columns.length;

    for (int i = 0; i < numColumns; ++i) {
      statisticsList.add(new DescriptiveStatistics());
    }

    while ((dataString = dataReader.readLine()) != null) {
      String[] dataArray = dataString.trim().split(" ");

      if (dataArray.length != numColumns) {
        throw new RuntimeException(
            "Row has missing columns: " + Arrays.toString(dataArray) + " Expected: " + numColumns + " columns.");
      }

      for (int i = 0; i < dataArray.length; ++i) {
        double data = Double.valueOf(dataArray[i]);
        statisticsList.get(i).addValue(data);
      }
    }

    for (int i = 0; i < numColumns; i++) {
      LOGGER.info("Stats: {}: {}", columns[i], statisticsList.get(i).toString().replace("\n", "\t"));
    }
  }

  public static void main(String[] args)
      throws IOException {
    if (args.length != 1) {
      LOGGER.error("Invalid arguments.");
      LOGGER.info("Usage: <exec> <data_file>");
    }

    try {
      String dataFile = args[0];
      StatsGenerator perfReportGenerator = new StatsGenerator();
      perfReportGenerator.generateReport(dataFile);
    } catch (Exception e) {
      LOGGER.error("Exception caught: ", e);
    }
  }
}
