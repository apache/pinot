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
package com.linkedin.pinot.tools.query.comparison;

import java.io.File;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class for comparing query results from star tree index to that from pinot index.
 * - Sets up two instances of Pinot, one with default pinot segments, and other with star tree segments.
 * - Uses QueryGenerator to generate and fire queries to the two clusters.
 * - Compares the response of the two and flags any errors.
 */
public class StarQueryComparison {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryComparison.class);
  private static final String STAR = ".star";

  private ClusterStarter _startTreeCluster;
  private ClusterStarter _refCluster;
  private String _segmentDirName;
  private String _tableName;
  private int _numQueries = 100;

  StarQueryComparison(QueryComparisonConfig config)
      throws Exception {

    _segmentDirName = config.getSegmentsDir();
    _tableName = config.getTableName();

    String starCluster = config.getClusterName() + STAR;
    String starSegmentDir = _segmentDirName + STAR;

    _startTreeCluster = new ClusterStarter(config);
    _startTreeCluster.setClusterName(starCluster);
    _startTreeCluster.setEnableStarTree(true);
    _startTreeCluster.setSegmentDirName(starSegmentDir);

    String controllerPort = config.getRefControllerPort();
    String brokerHost = config.getRefBrokerHost();
    String brokerPort = config.getRefBrokerPort();
    String serverPort = config.getRefServerPort();

    _refCluster =
        new ClusterStarter(config).setControllerPort(controllerPort).setBrokerHost(brokerHost).setBrokerPort(brokerPort)
            .setServerPort(serverPort).setStartZookeeper(true);
  }

  /**
   * - Launch the Pinot clusters with default segments and star tree segments.
   * - Run queries and compare results.
   * @return True if all tests passed, false otherwise
   * @throws Exception
   */
  public boolean run()
      throws Exception {

    _startTreeCluster.start();
    _refCluster.start();

    SegmentInfoProvider segmentInfoProvider = new SegmentInfoProvider(_segmentDirName);
    StarTreeQueryGenerator
        queryGenerator = new StarTreeQueryGenerator(_tableName, segmentInfoProvider.getSingleValueDimensionColumns(),
        segmentInfoProvider.getMetricColumns(), segmentInfoProvider.getSingleValueDimensionValuesMap());

    boolean ret = true;
    for (int i = 0; i < _numQueries; i++) {
      String query = queryGenerator.nextQuery();
      LOGGER.info("QUERY: {}", query);

      JSONObject refResponse = new JSONObject(_refCluster.query(query));
      JSONObject starTreeResponse = new JSONObject((_startTreeCluster.query(query)));

      if (QueryComparison.compare(refResponse, starTreeResponse, false)) {
        LOGGER.error("Comparison PASSED: {}", query);
      } else {
        ret = false;
        LOGGER.error("Comparison FAILED: {}", query);
        LOGGER.info("Ref Response: {}", _refCluster.query(query));
        LOGGER.info("StarTree Response: {}", _startTreeCluster.query(query));
      }
    }

    return ret;
  }

  /**
   * Launch the star tree query comparator based on the passed in configuration.
   * Configuration file must contain the following, all other values are optional:
   * - input.data.dir: Input directory containing avro files
   * - schema.file.name: Pinot schema file
   * - table.name: Table name
   *
   * @param args
   */
  public static void main(String[] args) {
    if (args.length != 1) {
      LOGGER.info("Usage: <exec> <config_file_path>");
      System.exit(1);
    }

    File configFile = new File(args[0]);
    try {
      StarQueryComparison comparator = new StarQueryComparison(new QueryComparisonConfig(configFile));
      comparator.run();
    } catch (Exception e) {
      LOGGER.error("Exception caught when comparing queries", e);
    }
    System.exit(0);
  }
}
