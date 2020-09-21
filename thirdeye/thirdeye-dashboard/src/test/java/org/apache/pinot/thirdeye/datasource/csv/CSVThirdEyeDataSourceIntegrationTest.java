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

package org.apache.pinot.thirdeye.datasource.csv;

import org.apache.pinot.thirdeye.common.ThirdEyeConfiguration;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.dataframe.util.RequestContainer;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import java.net.URL;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class CSVThirdEyeDataSourceIntegrationTest {
  private DAOTestBase testDAOProvider;
  private DAORegistry daoRegistry;

  @BeforeMethod
  void beforeMethod() {
    testDAOProvider = DAOTestBase.getInstance();
    daoRegistry = DAORegistry.getInstance();
  }

  @AfterMethod(alwaysRun = true)
  void afterMethod() {
    testDAOProvider.cleanup();
  }

  @Test
  public void integrationTest() throws Exception{
    URL dataSourcesConfig = this.getClass().getResource("data-sources-config.yml");

    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();

    datasetConfigDTO.setDataset("business");
    datasetConfigDTO.setDataSource("CSVThirdEyeDataSource");
    datasetConfigDTO.setTimeDuration(1);
    datasetConfigDTO.setTimeUnit(TimeUnit.HOURS);

    daoRegistry.getDatasetConfigDAO().save(datasetConfigDTO);
    Assert.assertNotNull(datasetConfigDTO.getId());


    MetricConfigDTO configDTO = new MetricConfigDTO();
    configDTO.setName("views");
    configDTO.setDataset("business");
    configDTO.setAlias("business::views");

    daoRegistry.getMetricConfigDAO().save(configDTO);
    Assert.assertNotNull(configDTO.getId());

    ThirdEyeConfiguration thirdEyeConfiguration = new ThirdEyeConfiguration();
    thirdEyeConfiguration.setDataSources(dataSourcesConfig.toString());

    ThirdEyeCacheRegistry.initializeCaches(thirdEyeConfiguration);
    ThirdEyeCacheRegistry cacheRegistry = ThirdEyeCacheRegistry.getInstance();


    MetricSlice slice = MetricSlice.from(configDTO.getId(), 0, 7200000);
    RequestContainer requestContainer = DataFrameUtils.makeAggregateRequest(slice, Collections.<String>emptyList(), -1, "ref");
    ThirdEyeResponse response = cacheRegistry.getQueryCache().getQueryResult(requestContainer.getRequest());
    DataFrame df = DataFrameUtils.evaluateResponse(response, requestContainer);

    Assert.assertEquals(df.getDoubles(DataFrame.COL_VALUE).toList(), Collections.singletonList(1503d));
  }

}
