package org.apache.pinot.thirdeye.detection.cache;

import com.couchbase.client.java.document.json.JsonObject;
import org.apache.pinot.thirdeye.util.CacheUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CacheUtilTests {

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

    Assert.assertEquals(mappedDataPoint.getLong("time").longValue(), 1234567);
    Assert.assertEquals(mappedDataPoint.getLong("metricId").longValue(), 1);
    Assert.assertEquals(mappedDataPoint.getString(dataPoint.getMetricUrnHash()), "100.0");
  }

  @Test
  public void testBuildDocumentStructureShouldMapNullDataValueToZero() {
    dataPoint.setDataValue(null);
    JsonObject mappedDataPoint = CacheUtils.buildDocumentStructure(dataPoint);

    Assert.assertEquals(mappedDataPoint.getString(dataPoint.getMetricUrnHash()), "0");
  }

  @Test
  public void testBuildDocumentStructureShouldMapNullStringDataValueToZero() {
    dataPoint.setDataValue("null");
    JsonObject mappedDataPoint = CacheUtils.buildDocumentStructure(dataPoint);

    Assert.assertEquals(mappedDataPoint.getString(dataPoint.getMetricUrnHash()), "0");
  }

  @Test
  public void testBuildQuery() {
    jsonObject.put(BUCKET, bucketName)
        .put(METRIC_ID, metricId)
        .put(DIMENSION_KEY, CacheUtils.hashMetricUrn(metricUrn))
        .put(START, 100)
        .put(END, 200);

    String query = CacheUtils.buildQuery(jsonObject);
    String expectedQuery = "SELECT time, `624972944` FROM `TestBucket` WHERE metricId = 1 AND `624972944` IS NOT MISSING AND time BETWEEN 100 AND 200 ORDER BY time ASC";

    Assert.assertEquals(query, expectedQuery);
  }
}
