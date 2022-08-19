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
package org.apache.pinot.tools;

import java.util.Arrays;
import java.util.List;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Sets up a demo Pinot cluster with 1 zookeeper, 1 controller, 1 broker and 1 server
 * Sets up a demo Kafka cluster for real-time ingestion
 * It takes a directory as input
 * <code>
 *  rawData
 *    - 1.csv
 *  table_config.json
 *  schema.json
 *  ingestion_job_spec.json
 *  </code>
 */
public class GenericQuickstart extends Quickstart {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenericQuickstart.class);

  public GenericQuickstart() {
  }

  @Override
  public List<String> types() {
    return Arrays.asList("GENERIC");
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    String q1 = "select count(*) from starbucksStores limit 0";
    printStatus(Color.YELLOW, "Total number of documents in the table");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Color.GREEN, "***************************************************");

    String q2 = "select address, ST_DISTANCE(location_st_point, ST_Point(-122, 37, 1)) from starbucksStores"
        + " WHERE ST_DISTANCE(location_st_point, ST_Point(-122, 37, 1)) < 5000 limit 1000";
    printStatus(Color.YELLOW, "Starbucks stores within 5km of the given point in bay area");
    printStatus(Color.CYAN, "Query : " + q2);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q2)));
    printStatus(Color.GREEN, "***************************************************");

    String q3 = "select address, ST_DISTANCE(location_st_point, ST_Point(-122, 37, 1)) from starbucksStores"
        + " WHERE ST_DISTANCE(location_st_point, ST_Point(-122, 37, 1)) between 5000 and 10000 limit 1000";
    printStatus(Color.YELLOW, "Starbucks stores with distance of 5km to 10km from the given point in bay area");
    printStatus(Color.CYAN, "Query : " + q3);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q3)));
    printStatus(Color.GREEN, "***************************************************");
  }

  public void execute()
      throws Exception {
    startKafka();
    super.execute();
  }

  public static void main(String[] args)
      throws Exception {
    new GenericQuickstart().execute();
  }
}
