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

package com.linkedin.thirdeye.datasource.csv;

import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.dataframe.util.RequestContainer;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
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

    Assert.assertEquals(df.getDoubles(DataFrameUtils.COL_VALUE).toList(), Collections.singletonList(1503d));
  }

}
