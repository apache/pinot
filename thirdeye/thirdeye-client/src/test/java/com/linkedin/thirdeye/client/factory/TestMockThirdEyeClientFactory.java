package com.linkedin.thirdeye.client.factory;

import static com.linkedin.thirdeye.client.factory.MockThirdEyeClientFactory.BUCKET_SIZE_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.factory.MockThirdEyeClientFactory.BUCKET_UNIT_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.factory.MockThirdEyeClientFactory.COLLECTION_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.factory.MockThirdEyeClientFactory.DIMENSION_COUNT_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.factory.MockThirdEyeClientFactory.DIMENSION_VALUE_CARDINALITY_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.factory.MockThirdEyeClientFactory.METRIC_COUNT_PROPERTY_KEY;
import static com.linkedin.thirdeye.client.factory.MockThirdEyeClientFactory.METRIC_VALUE_RANGE_PROPERTY_KEY;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.client.MockThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeClient;

public class TestMockThirdEyeClientFactory {
  private MockThirdEyeClientFactory factory;

  public static final String DEFAULT_COLLECTION = "mockCollection";
  public static final String DEFAULT_DIMENSION_COUNT = "3";
  public static final String DEFAULT_METRIC_COUNT = "5";
  public static final String DEFAULT_BUCKET_SIZE = "1";
  public static final String DEFAULT_BUCKET_UNIT = "HOURS";
  public static final String DEFAULT_METRIC_VALUE_RANGE = "2500000";
  public static final String DEFAULT_DIMENSION_VALUE_CARDINALITY = "5";

  @BeforeMethod
  public void setup() {
    factory = new MockThirdEyeClientFactory();
    factory.configureCache(null);
  }

  @Test(dataProvider = "validProps")
  public void getRawClient(Properties props) throws Exception {
    ThirdEyeClient client = factory.getRawClient(props);
    Assert.assertTrue(client instanceof MockThirdEyeClient);
  }

  @Test(dataProvider = "invalidProps", expectedExceptions = IllegalArgumentException.class)
  public void getRawClientErrors(Properties props) throws Exception {
    ThirdEyeClient client = factory.getRawClient(props);
  }

  @DataProvider(name = "validProps")
  public Object[][] validProps() {
    List<Properties> propList = new LinkedList<>();
    propList.add(createProps(DEFAULT_COLLECTION, DEFAULT_DIMENSION_COUNT, DEFAULT_METRIC_COUNT,
        DEFAULT_BUCKET_SIZE, DEFAULT_BUCKET_UNIT, DEFAULT_METRIC_VALUE_RANGE,
        DEFAULT_DIMENSION_VALUE_CARDINALITY));
    return convertToObjectArr2(propList);
  }

  @DataProvider(name = "invalidProps")
  public Object[][] invalidProps() {
    List<Properties> propList = new LinkedList<>();
    propList
        .add(createProps(null, DEFAULT_DIMENSION_COUNT, DEFAULT_METRIC_COUNT, DEFAULT_BUCKET_SIZE,
            DEFAULT_BUCKET_UNIT, DEFAULT_METRIC_VALUE_RANGE, DEFAULT_DIMENSION_VALUE_CARDINALITY));
    propList.add(createProps(DEFAULT_COLLECTION, null, DEFAULT_METRIC_COUNT, DEFAULT_BUCKET_SIZE,
        DEFAULT_BUCKET_UNIT, DEFAULT_METRIC_VALUE_RANGE, DEFAULT_DIMENSION_VALUE_CARDINALITY));
    propList.add(createProps(DEFAULT_COLLECTION, DEFAULT_DIMENSION_COUNT, null, DEFAULT_BUCKET_SIZE,
        DEFAULT_BUCKET_UNIT, DEFAULT_METRIC_VALUE_RANGE, DEFAULT_DIMENSION_VALUE_CARDINALITY));
    propList
        .add(createProps(DEFAULT_COLLECTION, DEFAULT_DIMENSION_COUNT, DEFAULT_METRIC_COUNT, null,
            DEFAULT_BUCKET_UNIT, DEFAULT_METRIC_VALUE_RANGE, DEFAULT_DIMENSION_VALUE_CARDINALITY));
    propList.add(createProps(DEFAULT_COLLECTION, DEFAULT_DIMENSION_COUNT, DEFAULT_METRIC_COUNT,
        DEFAULT_BUCKET_SIZE, null, DEFAULT_METRIC_VALUE_RANGE,
        DEFAULT_DIMENSION_VALUE_CARDINALITY));
    // metric value range and dimension value cardinality are optional/with defaults

    return convertToObjectArr2(propList);
  }

  private Properties createProps(String collection, String dimensionCount, String metricCount,
      String bucketSize, String bucketUnit, String metricValueRange,
      String dimensionValueCardinality) {
    Properties props = new Properties();
    addIfNotNull(props, COLLECTION_PROPERTY_KEY, collection);
    addIfNotNull(props, DIMENSION_COUNT_PROPERTY_KEY, dimensionCount);
    addIfNotNull(props, METRIC_COUNT_PROPERTY_KEY, metricCount);
    addIfNotNull(props, BUCKET_SIZE_PROPERTY_KEY, bucketSize);
    addIfNotNull(props, BUCKET_UNIT_PROPERTY_KEY, bucketUnit);
    addIfNotNull(props, METRIC_VALUE_RANGE_PROPERTY_KEY, metricValueRange);
    addIfNotNull(props, DIMENSION_VALUE_CARDINALITY_PROPERTY_KEY, dimensionValueCardinality);
    return props;
  }

  private void addIfNotNull(Properties props, String key, String value) {
    if (value != null) {
      props.put(key, value);
    }
  }

  private Object[][] convertToObjectArr2(List<Properties> propList) {
    List<Object[]> propsWrapperList = new LinkedList<>();
    for (Properties props : propList) {
      propsWrapperList.add(new Object[] {
          props
      });
    }
    return propsWrapperList.toArray(new Object[propsWrapperList.size()][]);
  }
}
