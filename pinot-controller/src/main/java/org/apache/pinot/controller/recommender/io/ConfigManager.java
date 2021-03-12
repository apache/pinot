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
package org.apache.pinot.controller.recommender.io;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.controller.recommender.rules.io.FlaggedQueries;
import org.apache.pinot.controller.recommender.rules.io.configs.IndexConfig;
import org.apache.pinot.controller.recommender.rules.io.configs.PartitionConfig;


/**
 * Manager for the overwritten configs in the input and output
 * overwritten configs:
 * Devs/sres/advanced user want to use the rule engine to recommend configs.
 * However, based on their experience or due to a special optimization for a use case,
 * they know that it will help to have inverted index on a particular column.
 * But they still want to run the engine to recommend inverted indexes on other columns (if applicable) and recommend other configs (sorted, bloom etc).
 * The engine will do it's job of recommending by taking into account the overwritten config and honoring it.
 */
public class ConfigManager {
  IndexConfig _indexConfig = new IndexConfig();
  PartitionConfig _partitionConfig = new PartitionConfig();
  FlaggedQueries _flaggedQueries = new FlaggedQueries();
  Map<String, Map<String, String>> _realtimeProvisioningRecommendations = new HashMap<>();

  @JsonSetter(nulls = Nulls.SKIP)
  public void setIndexConfig(IndexConfig indexConfig) {
    _indexConfig = indexConfig;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setPartitionConfig(PartitionConfig partitionConfig) {
    _partitionConfig = partitionConfig;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setFlaggedQueries(FlaggedQueries flaggedQueries) {
    _flaggedQueries = flaggedQueries;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setRealtimeProvisioningRecommendations(
      Map<String, Map<String, String>> realtimeProvisioningRecommendation) {
    _realtimeProvisioningRecommendations = realtimeProvisioningRecommendation;
  }

  public IndexConfig getIndexConfig() {
    return _indexConfig;
  }

  public PartitionConfig getPartitionConfig() {
    return _partitionConfig;
  }

  public FlaggedQueries getFlaggedQueries() {
    return _flaggedQueries;
  }

  public Map<String, Map<String, String>> getRealtimeProvisioningRecommendations() {
    return _realtimeProvisioningRecommendations;
  }
}
