package com.linkedin.thirdeye.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class StarTreeRecordStoreFactoryCircularBufferImpl implements StarTreeRecordStoreFactory {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final TypeReference TYPE_REFERENCE =
      new TypeReference<Map<String, Map<String, Integer>>>() {
      };

  private StarTreeConfig config;
  private List<DimensionSpec> dimensionSpecs;
  private List<MetricSpec> metricSpecs;

  private File rootDir;
  private int numTimeBuckets;

  @Override
  public void init(File rootDir, StarTreeConfig starTreeConfig, Properties recordStoreConfig) {
    this.config = starTreeConfig;
    this.rootDir = rootDir;
    this.numTimeBuckets = (int) starTreeConfig.getTime().getBucket().getUnit().convert(
        starTreeConfig.getTime().getRetention().getSize(),
        starTreeConfig.getTime().getRetention().getUnit());
  }

  @Override
  public StarTreeRecordStore createRecordStore(UUID nodeId) {
    File indexFile = new File(rootDir, nodeId.toString() + StarTreeConstants.INDEX_FILE_SUFFIX);

    Map<String, Map<String, Integer>> forwardIndex;
    try {
      forwardIndex = OBJECT_MAPPER.readValue(new FileInputStream(indexFile), TYPE_REFERENCE);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }

    File bufferFile = new File(rootDir, nodeId.toString() + StarTreeConstants.BUFFER_FILE_SUFFIX);

    return new StarTreeRecordStoreCircularBufferImpl(nodeId, bufferFile, config, forwardIndex,
        numTimeBuckets);
  }

  @Override
  public void close() throws IOException {
    // NOP
  }
}
