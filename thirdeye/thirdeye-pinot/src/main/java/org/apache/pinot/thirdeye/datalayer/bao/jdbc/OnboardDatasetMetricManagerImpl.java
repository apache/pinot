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
 */

package org.apache.pinot.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.List;
import org.apache.pinot.thirdeye.datalayer.bao.OnboardDatasetMetricManager;
import org.apache.pinot.thirdeye.datalayer.dao.GenericPojoDao;
import org.apache.pinot.thirdeye.datalayer.dto.OnboardDatasetMetricDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.OnboardDatasetMetricBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;

@Singleton
public class OnboardDatasetMetricManagerImpl extends AbstractManagerImpl<OnboardDatasetMetricDTO>
    implements OnboardDatasetMetricManager {

  @Inject
  public OnboardDatasetMetricManagerImpl(GenericPojoDao genericPojoDao) {
    super(OnboardDatasetMetricDTO.class, OnboardDatasetMetricBean.class, genericPojoDao);
  }

  @Override
  public List<OnboardDatasetMetricDTO> findByDataSource(String dataSource) {
    Predicate dataSourcePredicate = Predicate.EQ("dataSource", dataSource);
    return findByPredicate(dataSourcePredicate);
  }

  @Override
  public List<OnboardDatasetMetricDTO> findByDataSourceAndOnboarded(String dataSource,
      boolean onboarded) {
    Predicate dataSourcePredicate = Predicate.EQ("dataSource", dataSource);
    Predicate onboardedPredicate = Predicate.EQ("onboarded", onboarded);
    Predicate predicate = Predicate.AND(dataSourcePredicate, onboardedPredicate);
    return findByPredicate(predicate);
  }

  @Override
  public List<OnboardDatasetMetricDTO> findByDataset(String datasetName) {
    Predicate predicate = Predicate.EQ("datasetName", datasetName);
    return findByPredicate(predicate);
  }

  @Override
  public List<OnboardDatasetMetricDTO> findByDatasetAndOnboarded(String datasetName,
      boolean onboarded) {
    Predicate datasetNamePredicate = Predicate.EQ("datasetName", datasetName);
    Predicate onboardedPredicate = Predicate.EQ("onboarded", onboarded);
    Predicate predicate = Predicate.AND(datasetNamePredicate, onboardedPredicate);
    return findByPredicate(predicate);
  }

  @Override
  public List<OnboardDatasetMetricDTO> findByMetric(String metricName) {
    Predicate predicate = Predicate.EQ("metricName", metricName);
    return findByPredicate(predicate);
  }

  @Override
  public List<OnboardDatasetMetricDTO> findByMetricAndOnboarded(String metricName, boolean onboarded) {
    Predicate metricNamePredicate = Predicate.EQ("metricName", metricName);
    Predicate onboardedPredicate = Predicate.EQ("onboarded", onboarded);
    Predicate predicate = Predicate.AND(metricNamePredicate, onboardedPredicate);
    return findByPredicate(predicate);
  }

  @Override
  public List<OnboardDatasetMetricDTO> findByDatasetAndDatasource(String datasetName,
      String dataSource) {
    Predicate datasetNamePredicate = Predicate.EQ("datasetName", datasetName);
    Predicate dataSourcePredicate = Predicate.EQ("dataSource", dataSource);
    Predicate predicate = Predicate.AND(datasetNamePredicate, dataSourcePredicate);
    return findByPredicate(predicate);
  }

  @Override
  public List<OnboardDatasetMetricDTO> findByDatasetAndDatasourceAndOnboarded(String datasetName,
      String dataSource, boolean onboarded) {
    Predicate datasetNamePredicate = Predicate.EQ("datasetName", datasetName);
    Predicate dataSourcePredicate = Predicate.EQ("dataSource", dataSource);
    Predicate onboardedPredicate = Predicate.EQ("onboarded", onboarded);
    Predicate predicate = Predicate.AND(datasetNamePredicate, dataSourcePredicate, onboardedPredicate);
    return findByPredicate(predicate);
  }


}
