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

import org.apache.pinot.controller.util.AutoAddInvertedIndex;
import picocli.CommandLine;


@SuppressWarnings("FieldCanBeLocal")
@CommandLine.Command(name = "AutoAddInvertedIndex", description = "Automatically add inverted index to tables based "
                                                                  + "on the settings. Currently only support 'QUERY' "
                                                                  + "strategy")
public class AutoAddInvertedIndexTool extends AbstractBaseCommand implements Command {
  @CommandLine.Option(names = {"-zkAddress"}, required = true, description = "Address of the Zookeeper (host:port)")
  private String _zkAddress;

  @CommandLine.Option(names = {"-clusterName"}, required = true, description = "Pinot cluster name")
  private String _clusterName;

  @CommandLine.Option(names = {"-controllerAddress"}, required = true,
      description = "Address of the Pinot controller (host:port)")
  private String _controllerAddress;

  @CommandLine.Option(names = {"-brokerAddress"}, required = true,
      description = "Address of the Pinot broker (host:port)")
  private String _brokerAddress;

  @CommandLine.Option(names = {"-strategy"}, required = false,
      description = "Strategy to add inverted index (QUERY), default: QUERY")
  private AutoAddInvertedIndex.Strategy _strategy = AutoAddInvertedIndex.Strategy.QUERY;

  @CommandLine.Option(names = {"-mode"}, required = false,
      description = "Mode to add inverted index (NEW|REMOVE|REFRESH|APPEND), default: NEW")
  private AutoAddInvertedIndex.Mode _mode = AutoAddInvertedIndex.Mode.NEW;

  @CommandLine.Option(names = {"-tableNamePattern"}, required = false,
      description = "Optional table name pattern trigger to add inverted index, default: null (match any table name)")
  private String _tableNamePattern = null;

  @CommandLine.Option(names = {"-tableSizeThreshold"}, required = false,
      description = "Optional table size threshold to trigger adding inverted index, default: "
          + AutoAddInvertedIndex.DEFAULT_TABLE_SIZE_THRESHOLD)
  private long _tableSizeThreshold = AutoAddInvertedIndex.DEFAULT_TABLE_SIZE_THRESHOLD;

  @CommandLine.Option(names = {"-cardinalityThreshold"}, required = false,
      description = "Optional cardinality threshold to trigger adding inverted index, default: "
          + AutoAddInvertedIndex.DEFAULT_CARDINALITY_THRESHOLD)
  private long _cardinalityThreshold = AutoAddInvertedIndex.DEFAULT_CARDINALITY_THRESHOLD;

  @CommandLine.Option(names = {"-maxNumInvertedIndex"}, required = false,
      description = "Optional max number of inverted index added, default: "
          + AutoAddInvertedIndex.DEFAULT_MAX_NUM_INVERTED_INDEX_ADDED)
  private int _maxNumInvertedIndex = AutoAddInvertedIndex.DEFAULT_MAX_NUM_INVERTED_INDEX_ADDED;

  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public boolean execute()
      throws Exception {
    AutoAddInvertedIndex autoAddInvertedIndex =
        new AutoAddInvertedIndex(_zkAddress, _clusterName, _controllerAddress, _brokerAddress, _strategy, _mode);
    autoAddInvertedIndex
        .overrideDefaultSettings(_tableNamePattern, _tableSizeThreshold, _cardinalityThreshold, _maxNumInvertedIndex);
    autoAddInvertedIndex.run();
    return true;
  }
}
