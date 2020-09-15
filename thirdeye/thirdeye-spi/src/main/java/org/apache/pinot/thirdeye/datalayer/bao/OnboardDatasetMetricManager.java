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

import org.apache.pinot.thirdeye.datalayer.dto.OnboardDatasetMetricDTO;

import java.util.List;

public interface OnboardDatasetMetricManager extends AbstractManager<OnboardDatasetMetricDTO> {

  List<OnboardDatasetMetricDTO> findByDataSource(String dataSource);
  List<OnboardDatasetMetricDTO> findByDataSourceAndOnboarded(String dataSource, boolean onboarded);
  List<OnboardDatasetMetricDTO> findByDataset(String datasetName);
  List<OnboardDatasetMetricDTO> findByDatasetAndOnboarded(String datasetName, boolean onboarded);
  List<OnboardDatasetMetricDTO> findByMetric(String metricName);
  List<OnboardDatasetMetricDTO> findByMetricAndOnboarded(String metricName, boolean onboarded);
  List<OnboardDatasetMetricDTO> findByDatasetAndDatasource(String datasetName, String dataSource);
  List<OnboardDatasetMetricDTO> findByDatasetAndDatasourceAndOnboarded(String datasetName, String dataSource, boolean onboarded);


}
