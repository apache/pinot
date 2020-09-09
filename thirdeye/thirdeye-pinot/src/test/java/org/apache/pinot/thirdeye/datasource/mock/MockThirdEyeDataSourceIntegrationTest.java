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

package org.apache.pinot.thirdeye.datasource.mock;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.apache.pinot.thirdeye.common.ThirdEyeConfiguration;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.dataframe.util.RequestContainer;
import org.apache.pinot.thirdeye.dataframe.util.TimeSeriesRequestContainer;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MockThirdEyeDataSourceIntegrationTest {
  private DAOTestBase testDAOProvider;
  private DAORegistry daoRegistry;

  private Long metricPurchasesId;
  private Long metricRevenueId;
  private Long metricAdImpressionsId;
  private Long metricPageViewsId;
  private ThirdEyeCacheRegistry cacheRegistry;

  private long timestamp;

  @BeforeClass
  void beforeMethod() throws Exception {
    this.testDAOProvider = DAOTestBase.getInstance();
    this.daoRegistry = DAORegistry.getInstance();

    URL dataSourcesConfig = this.getClass().getResource("data-sources-config.yml");

    // NOTE: MockThirdEyeDataSource expects the first IDs to be reserved for metrics

    // metric business::purchases
    MetricConfigDTO metricPurchases = new MetricConfigDTO();
    metricPurchases.setName("purchases");
    metricPurchases.setDataset("business");
    metricPurchases.setAlias("business::purchases");
    this.metricPurchasesId = this.daoRegistry.getMetricConfigDAO().save(metricPurchases);
    Assert.assertNotNull(this.metricPurchasesId);

    // metric business::revenue
    MetricConfigDTO metricRevenue = new MetricConfigDTO();
    metricRevenue.setName("revenue");
    metricRevenue.setDataset("business");
    metricRevenue.setAlias("business::revenue");
    this.metricRevenueId = this.daoRegistry.getMetricConfigDAO().save(metricRevenue);
    Assert.assertNotNull(this.metricRevenueId);

    // metric tracking::adImpressions
    MetricConfigDTO metricAdImpressions = new MetricConfigDTO();
    metricAdImpressions.setName("adImpressions");
    metricAdImpressions.setDataset("tracking");
    metricAdImpressions.setAlias("tracking::adImpressions");
    this.metricAdImpressionsId = this.daoRegistry.getMetricConfigDAO().save(metricAdImpressions);
    Assert.assertNotNull(this.metricAdImpressionsId);

    // metric tracking::pageViews
    MetricConfigDTO metricPageViews = new MetricConfigDTO();
    metricPageViews.setName("pageViews");
    metricPageViews.setDataset("tracking");
    metricPageViews.setAlias("tracking::pageViews");
    this.metricPageViewsId = this.daoRegistry.getMetricConfigDAO().save(metricPageViews);
    Assert.assertNotNull(this.metricPageViewsId);

    // dataset business
    DatasetConfigDTO datasetBusiness = new DatasetConfigDTO();
    datasetBusiness.setDataset("business");
    datasetBusiness.setDataSource("MockThirdEyeDataSource");
    datasetBusiness.setTimeDuration(1);
    datasetBusiness.setTimeUnit(TimeUnit.DAYS);
    datasetBusiness.setTimezone("America/Los_Angeles");
    datasetBusiness.setDimensions(Arrays.asList("browser", "country"));
    this.daoRegistry.getDatasetConfigDAO().save(datasetBusiness);
    Assert.assertNotNull(datasetBusiness.getId());

    // dataset tracking
    DatasetConfigDTO datasetTracking = new DatasetConfigDTO();
    datasetTracking.setDataset("tracking");
    datasetTracking.setDataSource("MockThirdEyeDataSource");
    datasetTracking.setTimeDuration(1);
    datasetTracking.setTimeUnit(TimeUnit.HOURS);
    datasetTracking.setTimezone("America/Los_Angeles");
    datasetTracking.setDimensions(Arrays.asList("browser", "country", "platform"));
    this.daoRegistry.getDatasetConfigDAO().save(datasetTracking);
    Assert.assertNotNull(datasetTracking.getId());

    // data sources and caches
    this.timestamp = System.currentTimeMillis();
    ThirdEyeConfiguration thirdEyeConfiguration = new ThirdEyeConfiguration();
    thirdEyeConfiguration.setDataSources(dataSourcesConfig.toString());

    ThirdEyeCacheRegistry.initializeCaches(thirdEyeConfiguration);
    this.cacheRegistry = ThirdEyeCacheRegistry.getInstance();
  }

  @AfterClass(alwaysRun = true)
  void afterMethod() {
    this.testDAOProvider.cleanup();
  }

  @Test
  public void testAggregation() throws Exception {
    MetricSlice slice = MetricSlice.from(this.metricPageViewsId, this.timestamp - 7200000, this.timestamp);
    RequestContainer requestContainer = DataFrameUtils.makeAggregateRequest(slice, Collections.<String>emptyList(), -1, "ref");
    ThirdEyeResponse response = this.cacheRegistry.getQueryCache().getQueryResult(requestContainer.getRequest());
    DataFrame df = DataFrameUtils.evaluateResponse(response, requestContainer);

    Assert.assertTrue(df.getDouble(DataFrame.COL_VALUE, 0) > 0);
  }

  @Test
  public void testBreakdown() throws Exception {
    MetricSlice slice = MetricSlice.from(this.metricRevenueId, this.timestamp - TimeUnit.HOURS.toMillis(25), this.timestamp); // allow for DST
    RequestContainer requestContainer = DataFrameUtils.makeAggregateRequest(slice, Arrays.asList("country", "browser"), -1, "ref");
    ThirdEyeResponse response = this.cacheRegistry.getQueryCache().getQueryResult(requestContainer.getRequest());
    DataFrame df = DataFrameUtils.evaluateResponse(response, requestContainer);

    Assert.assertEquals(df.size(), 9);
    Assert.assertEquals(new HashSet<>(df.getStrings("country").toList()), new HashSet<>(Arrays.asList("ca", "mx", "us")));
    Assert.assertEquals(new HashSet<>(df.getStrings("browser").toList()), new HashSet<>(Arrays.asList("chrome", "edge", "safari")));
    for (int i = 0; i < df.size(); i++) {
      Assert.assertTrue(df.getDouble(DataFrame.COL_VALUE, i) >= 0);
    }
  }

  @Test
  public void testTimeSeries() throws Exception {
    MetricSlice slice = MetricSlice.from(this.metricPageViewsId, this.timestamp - 7200000, this.timestamp);
    TimeSeriesRequestContainer requestContainer = DataFrameUtils.makeTimeSeriesRequest(slice, "ref");
    ThirdEyeResponse response = this.cacheRegistry.getQueryCache().getQueryResult(requestContainer.getRequest());
    DataFrame df = DataFrameUtils.evaluateResponse(response, requestContainer);

    Assert.assertEquals(df.size(), 2);
    Assert.assertTrue(df.getLong(DataFrame.COL_TIME, 0) > 0);
    Assert.assertTrue(df.getDouble(DataFrame.COL_VALUE, 0) > 0);
    Assert.assertTrue(df.getLong(DataFrame.COL_TIME, 1) > 0);
    Assert.assertTrue(df.getDouble(DataFrame.COL_VALUE, 1) > 0);
  }

  @Test
  public void testAggregationWithFilter() throws Exception {
    SetMultimap<String, String> filtersBasic = HashMultimap.create();
    filtersBasic.put("browser", "safari");
    filtersBasic.put("browser", "firefox");

    SetMultimap<String, String> filtersMobile = HashMultimap.create(filtersBasic);
    filtersMobile.put("platform", "mobile");

    SetMultimap<String, String> filtersDesktop = HashMultimap.create(filtersBasic);
    filtersDesktop.put("platform", "desktop");

    MetricSlice sliceBasic = MetricSlice.from(this.metricAdImpressionsId, this.timestamp - 7200000, this.timestamp, filtersBasic);
    MetricSlice sliceMobile = MetricSlice.from(this.metricAdImpressionsId, this.timestamp - 7200000, this.timestamp, filtersMobile);
    MetricSlice sliceDesktop = MetricSlice.from(this.metricAdImpressionsId, this.timestamp - 7200000, this.timestamp, filtersDesktop);

    RequestContainer reqBasic = DataFrameUtils.makeAggregateRequest(sliceBasic, Collections.<String>emptyList(), -1, "ref");
    ThirdEyeResponse resBasic = this.cacheRegistry.getQueryCache().getQueryResult(reqBasic.getRequest());
    DataFrame dfBasic = DataFrameUtils.evaluateResponse(resBasic, reqBasic);

    RequestContainer reqMobile = DataFrameUtils.makeAggregateRequest(sliceMobile, Collections.<String>emptyList(), -1, "ref");
    ThirdEyeResponse resMobile = this.cacheRegistry.getQueryCache().getQueryResult(reqMobile.getRequest());
    DataFrame dfMobile = DataFrameUtils.evaluateResponse(resMobile, reqMobile);

    RequestContainer reqDesktop = DataFrameUtils.makeAggregateRequest(sliceDesktop, Collections.<String>emptyList(), -1, "ref");
    ThirdEyeResponse resDesktop = this.cacheRegistry.getQueryCache().getQueryResult(reqDesktop.getRequest());
    DataFrame dfDesktop = DataFrameUtils.evaluateResponse(resDesktop, reqDesktop);

    Assert.assertTrue(dfBasic.getDouble(DataFrame.COL_VALUE, 0) >= dfMobile.getDouble(DataFrame.COL_VALUE, 0));
    Assert.assertTrue(dfBasic.getDouble(DataFrame.COL_VALUE, 0) >= dfDesktop.getDouble(DataFrame.COL_VALUE, 0));
    Assert.assertEquals(dfBasic.getDouble(DataFrame.COL_VALUE, 0),
        dfDesktop.getDouble(DataFrame.COL_VALUE, 0) + dfMobile.getDouble(DataFrame.COL_VALUE, 0));
  }
}
