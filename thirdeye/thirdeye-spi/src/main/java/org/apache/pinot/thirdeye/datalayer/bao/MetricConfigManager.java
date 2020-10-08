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
 *
 */

package org.apache.pinot.thirdeye.datalayer.bao;

import java.util.List;

import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import java.util.Set;


public interface MetricConfigManager extends AbstractManager<MetricConfigDTO> {

  List<MetricConfigDTO> findByDataset(String dataset);
  MetricConfigDTO findByMetricAndDataset(String metricName, String dataset);
  MetricConfigDTO findByAliasAndDataset(String alias, String dataset);
  List<MetricConfigDTO> findActiveByDataset(String dataset);
  List<MetricConfigDTO> findWhereNameOrAliasLikeAndActive(String name);
  List<MetricConfigDTO> findWhereAliasLikeAndActive(Set<String> aliasParts);
  List<MetricConfigDTO> findByMetricName(String metricName);

}
