package com.linkedin.thirdeye.client.cache;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheLoader;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.dashboard.configs.AbstractConfigDAO;

public class CollectionSchemaCacheLoader extends CacheLoader<String, CollectionSchema> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CollectionSchemaCacheLoader.class);

  private static final int GRANULARITY_SIZE = 1;

  private AbstractConfigDAO<CollectionSchema> collectionSchemaDAO;

  public CollectionSchemaCacheLoader(PinotThirdEyeClientConfig pinotThirdeyeClientConfig,
      AbstractConfigDAO<CollectionSchema> collectionSchemaDAO) {
    this.collectionSchemaDAO = collectionSchemaDAO;
  }

  @Override
  public CollectionSchema load(String collection) throws Exception {
    CollectionSchema collectionSchema = getCollectionSchema(collection);
    return collectionSchema;
  }

  public CollectionSchema getCollectionSchema(String collection) throws Exception {
    CollectionSchema collectionSchema = null;
    collectionSchema = collectionSchemaDAO.findById(collection);
    if (collectionSchema == null) {
      // get from schema endpoint
      Schema schema = ThirdEyeCacheRegistry.getInstance().getSchemaCache().get(collection);
      List<DimensionSpec> dimSpecs = fromDimensionFieldSpecs(schema.getDimensionFieldSpecs());
      List<MetricSpec> metricSpecs = fromMetricFieldSpecs(schema.getMetricFieldSpecs());
      TimeSpec timeSpec = fromTimeFieldSpec(schema.getTimeFieldSpec(), null);
      CollectionSchema config = new CollectionSchema.Builder().setCollection(collection)
          .setDimensions(dimSpecs).setMetrics(metricSpecs).setTime(timeSpec).build();
      return config;
    }

    return collectionSchema;
  }

  private List<DimensionSpec> fromDimensionFieldSpecs(List<DimensionFieldSpec> specs) {
    List<DimensionSpec> results = new ArrayList<>(specs.size());
    for (DimensionFieldSpec dimensionFieldSpec : specs) {
      DimensionSpec dimensionSpec = new DimensionSpec(dimensionFieldSpec.getName());
      results.add(dimensionSpec);
    }
    return results;
  }

  private List<MetricSpec> fromMetricFieldSpecs(List<MetricFieldSpec> specs) {
    ArrayList<MetricSpec> results = new ArrayList<>(specs.size());
    for (MetricFieldSpec metricFieldSpec : specs) {
      MetricSpec metricSpec = getMetricType(metricFieldSpec);
      results.add(metricSpec);
    }
    return results;
  }

  private MetricSpec getMetricType(MetricFieldSpec metricFieldSpec) {
    DataType dataType = metricFieldSpec.getDataType();
    MetricType metricType;
    switch (dataType) {
    case BOOLEAN:
    case BYTE:
    case BYTE_ARRAY:
    case CHAR:
    case CHAR_ARRAY:
    case DOUBLE_ARRAY:
    case FLOAT_ARRAY:
    case INT_ARRAY:
    case LONG_ARRAY:
    case OBJECT:
    case SHORT_ARRAY:
    case STRING:
    case STRING_ARRAY:
    default:
      throw new UnsupportedOperationException(dataType + " is not a supported metric type");
    case DOUBLE:
      metricType = MetricType.DOUBLE;
      break;
    case FLOAT:
      metricType = MetricType.FLOAT;
      break;
    case INT:
      metricType = MetricType.INT;
      break;
    case LONG:
      metricType = MetricType.LONG;
      break;
    case SHORT:
      metricType = MetricType.SHORT;
      break;

    }
    MetricSpec metricSpec = new MetricSpec(metricFieldSpec.getName(), metricType);
    return metricSpec;
  }

  private TimeSpec fromTimeFieldSpec(TimeFieldSpec timeFieldSpec, String format) {
    TimeGranularity outputGranularity = new TimeGranularity(GRANULARITY_SIZE,
        timeFieldSpec.getOutgoingGranularitySpec().getTimeType());
    TimeSpec spec =
        new TimeSpec(timeFieldSpec.getOutgoingTimeColumnName(), outputGranularity, format);
    return spec;
  }

}
