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


package org.apache.pinot.thirdeye.util;

import com.couchbase.client.java.document.json.JsonObject;
import org.apache.pinot.thirdeye.detection.cache.TimeSeriesDataPoint;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CacheUtilTest {

  private static final String BUCKET = "bucket";
  private static final String DIMENSION_KEY = "dimensionKey";
  private static final String METRIC_ID = "metricId";
  private static final String START = "start";
  private static final String END = "end";

  private static String metricUrn = "thirdeye:metric:1";
  private static String metricUrnHash = "624972944";
  private final long timestamp = 1234567;
  private final long metricId = 1;
  private final String dataValue = "100.0";
  private final String bucketName = "TestBucket";

  private TimeSeriesDataPoint dataPoint;
  private JsonObject jsonObject;


  @BeforeMethod
  public void beforeMethod() {
    dataPoint = new TimeSeriesDataPoint(metricUrn, timestamp, metricId, dataValue);
    jsonObject = JsonObject.create();
  }

  @AfterMethod
  public void afterMethod() {
    dataPoint = null;
    jsonObject = JsonObject.empty();
  }

  @Test
  public void testHashMetricUrn() {
    Assert.assertEquals(metricUrnHash, CacheUtils.hashMetricUrn(metricUrn));
  }

  @Test
  public void testBuildDocumentStructureShouldMapToJsonObject() {
    JsonObject mappedDataPoint = CacheUtils.buildDocumentStructure(dataPoint);

    Assert.assertEquals(mappedDataPoint.getLong("timestamp").longValue(), 1234567);
    Assert.assertEquals(mappedDataPoint.getLong("metricId").longValue(), 1);
    Assert.assertEquals(mappedDataPoint.getDouble(dataPoint.getMetricUrnHash()), (double)100);
  }

  @Test
  public void testBuildDocumentStructureShouldMapNullDataValueToZero() {
    dataPoint.setDataValue(null);
    JsonObject mappedDataPoint = CacheUtils.buildDocumentStructure(dataPoint);

    Assert.assertEquals(mappedDataPoint.getDouble(dataPoint.getMetricUrnHash()), (double)0);
  }

  @Test
  public void testBuildDocumentStructureShouldMapNullStringDataValueToZero() {
    dataPoint.setDataValue("null");
    JsonObject mappedDataPoint = CacheUtils.buildDocumentStructure(dataPoint);

    Assert.assertEquals(mappedDataPoint.getDouble(dataPoint.getMetricUrnHash()), (double)0);
  }

  @Test
  public void testBuildQuery() {
    jsonObject.put(BUCKET, bucketName)
        .put(METRIC_ID, metricId)
        .put(DIMENSION_KEY, CacheUtils.hashMetricUrn(metricUrn))
        .put(START, 100)
        .put(END, 200);

    String query = CacheUtils.buildQuery(jsonObject);
    String expectedQuery = "SELECT timestamp, `624972944` FROM `TestBucket` WHERE metricId = 1 AND `624972944` IS NOT MISSING AND timestamp BETWEEN 100 AND 200 ORDER BY time ASC";

    Assert.assertEquals(query, expectedQuery);
  }
}
