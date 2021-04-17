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
import org.kohsuke.args4j.Option;


@SuppressWarnings("FieldCanBeLocal")
public class AutoAddInvertedIndexTool extends AbstractBaseCommand implements Command {
  @Option(name = "-zkAddress", required = true, metaVar = "<string>", usage = "Address of the Zookeeper (host:port)")
  private String _zkAddress;

  @Option(name = "-clusterName", required = true, metaVar = "<string>", usage = "Pinot cluster name")
  private String _clusterName;

  @Option(name = "-controllerAddress", required = true, metaVar = "<string>",
      usage = "Address of the Pinot controller (host:port)")
  private String _controllerAddress;

  @Option(name = "-brokerAddress", required = true, metaVar = "<string>",
      usage = "Address of the Pinot broker (host:port)")
  private String _brokerAddress;

  @Option(name = "-strategy", required = false, metaVar = "<Strategy>",
      usage = "Strategy to add inverted index (QUERY), default: QUERY")
  private AutoAddInvertedIndex.Strategy _strategy = AutoAddInvertedIndex.Strategy.QUERY;

  @Option(name = "-mode", required = false, metaVar = "<Mode>",
      usage = "Mode to add inverted index (NEW|REMOVE|REFRESH|APPEND), default: NEW")
  private AutoAddInvertedIndex.Mode _mode = AutoAddInvertedIndex.Mode.NEW;

  @Option(name = "-tableNamePattern", required = false, metaVar = "<string>",
      usage = "Optional table name pattern to trigger adding inverted index, default: null (match any table name)")
  private String _tableNamePattern = null;

  @Option(name = "-tableSizeThreshold", required = false, metaVar = "<long>",
      usage = "Optional table size threshold to trigger adding inverted index, default: "
          + AutoAddInvertedIndex.DEFAULT_TABLE_SIZE_THRESHOLD)
  private long _tableSizeThreshold = AutoAddInvertedIndex.DEFAULT_TABLE_SIZE_THRESHOLD;

  @Option(name = "-cardinalityThreshold", required = false, metaVar = "<long>",
      usage = "Optional cardinality threshold to trigger adding inverted index, default: "
          + AutoAddInvertedIndex.DEFAULT_CARDINALITY_THRESHOLD)
  private long _cardinalityThreshold = AutoAddInvertedIndex.DEFAULT_CARDINALITY_THRESHOLD;

  @Option(name = "-maxNumInvertedIndex", required = false, metaVar = "<int>",
      usage = "Optional max number of inverted index added, default: "
          + AutoAddInvertedIndex.DEFAULT_MAX_NUM_INVERTED_INDEX_ADDED)
  private int _maxNumInvertedIndex = AutoAddInvertedIndex.DEFAULT_MAX_NUM_INVERTED_INDEX_ADDED;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
      usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public String description() {
    return "Automatically add inverted index to tables based on the settings. Currently only support 'QUERY' strategy";
  }

  @Override
  public boolean execute() throws Exception {
    AutoAddInvertedIndex autoAddInvertedIndex =
        new AutoAddInvertedIndex(_zkAddress, _clusterName, _controllerAddress, _brokerAddress, _strategy, _mode);
    autoAddInvertedIndex.overrideDefaultSettings(_tableNamePattern, _tableSizeThreshold, _cardinalityThreshold,
        _maxNumInvertedIndex);
    autoAddInvertedIndex.run();
    return true;
  }
}
