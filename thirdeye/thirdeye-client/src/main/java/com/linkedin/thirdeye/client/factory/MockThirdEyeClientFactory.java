package com.linkedin.thirdeye.client.factory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MockThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeClient;

public class MockThirdEyeClientFactory extends BaseThirdEyeClientFactory {

  public static final String STAR_TREE_CONFIG_FILE_PROPERTY_KEY = "starTreeConfigFile";
  public static final String COLLECTION_PROPERTY_KEY = "collection";
  public static final String DIMENSION_COUNT_PROPERTY_KEY = "numDimensions";
  public static final String METRIC_COUNT_PROPERTY_KEY = "numMetrics";
  public static final String BUCKET_SIZE_PROPERTY_KEY = "bucketSize";
  public static final String BUCKET_UNIT_PROPERTY_KEY = "bucketUnit";
  public static final String METRIC_VALUE_RANGE_PROPERTY_KEY = "metricValueRange";
  public static final String DIMENSION_VALUE_CARDINALITY_PROPERTY_KEY = "dimensionValueCardinality";

  @Override
  protected ThirdEyeClient getRawClient(Properties props) {
    int metricValueRange = MockThirdEyeClient.DEFAULT_METRIC_VALUE_RANGE;
    int dimensionValueCardinality = MockThirdEyeClient.DEFAULT_DIMENSION_VALUE_CARDINALITY;

    if (props.containsKey(METRIC_VALUE_RANGE_PROPERTY_KEY)) {
      metricValueRange = Integer.valueOf(props.getProperty(METRIC_VALUE_RANGE_PROPERTY_KEY));
    }

    if (props.containsKey(DIMENSION_VALUE_CARDINALITY_PROPERTY_KEY)) {
      dimensionValueCardinality =
          Integer.valueOf(props.getProperty(DIMENSION_VALUE_CARDINALITY_PROPERTY_KEY));
    }

    if (props.containsKey(STAR_TREE_CONFIG_FILE_PROPERTY_KEY)) {
      return MockThirdEyeClient.fromConfigFile(
          props.getProperty(STAR_TREE_CONFIG_FILE_PROPERTY_KEY), metricValueRange,
          dimensionValueCardinality);
    } else {
      assertContainsKeys(props, COLLECTION_PROPERTY_KEY, DIMENSION_COUNT_PROPERTY_KEY,
          METRIC_COUNT_PROPERTY_KEY, BUCKET_SIZE_PROPERTY_KEY, BUCKET_UNIT_PROPERTY_KEY);
      String collection = props.getProperty(COLLECTION_PROPERTY_KEY);
      int numDimensions = Integer.valueOf(props.getProperty(DIMENSION_COUNT_PROPERTY_KEY));
      int numMetrics = Integer.valueOf(props.getProperty(METRIC_COUNT_PROPERTY_KEY));
      int bucketSize = Integer.valueOf(props.getProperty(BUCKET_SIZE_PROPERTY_KEY));
      TimeUnit bucketUnit = TimeUnit.valueOf(props.getProperty(BUCKET_UNIT_PROPERTY_KEY));
      return MockThirdEyeClient.generateClientWithStarTreeProps(collection, numDimensions,
          numMetrics, new TimeGranularity(bucketSize, bucketUnit), metricValueRange,
          dimensionValueCardinality);
    }
  }

}
