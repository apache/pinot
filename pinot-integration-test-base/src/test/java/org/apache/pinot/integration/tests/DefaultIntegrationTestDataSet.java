/**
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
package org.apache.pinot.integration.tests;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.tools.utils.KafkaStarterUtils;


/**
 * Default integration test data set. for special test cases, one can extend and register a new integration test
 *
 */
public class DefaultIntegrationTestDataSet extends ClusterIntegrationTestDataAndQuerySet {

  // Default settings for Airline table.
  protected static final String DEFAULT_TABLE_NAME = "mytable";
  protected static final String DEFAULT_SCHEMA_NAME = "mytable";
  protected static final String DEFAULT_SCHEMA_FILE_NAME =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls.schema";
  protected static final String DEFAULT_TIME_COLUMN_NAME = "DaysSinceEpoch";
  protected static final String DEFAULT_AVRO_TAR_FILE_NAME =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls.tar.gz";
  protected static final long DEFAULT_COUNT_STAR_RESULT = 115545L;
  protected static final int DEFAULT_MAX_NUM_KAFKA_MESSAGES_PER_BATCH = 10000;
  protected static final List<String> DEFAULT_NO_DICTIONARY_COLUMNS =
      Arrays.asList("ActualElapsedTime", "ArrDelay", "DepDelay", "CRSDepTime");
  protected static final String DEFAULT_SORTED_COLUMN = "Carrier";
  protected static final List<String> DEFAULT_INVERTED_INDEX_COLUMNS = Arrays.asList("FlightNum", "Origin", "Quarter");
  protected static final List<String> DEFAULT_BLOOM_FILTER_COLUMNS = Arrays.asList("FlightNum", "Origin");
  protected static final List<String> DEFAULT_RANGE_INDEX_COLUMNS = Collections.singletonList("Origin");
  protected static final int DEFAULT_NUM_REPLICAS = 1;
  protected static final boolean DEFAULT_NULL_HANDLING_ENABLED = false;

  public DefaultIntegrationTestDataSet(ClusterTest clusterTest) {
    super(clusterTest);
  }

  /**
   * The following getters can be overridden to change default settings.
   */
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  public String getSchemaName() {
    return DEFAULT_SCHEMA_NAME;
  }

  public String getSchemaFileName() {
    return DEFAULT_SCHEMA_FILE_NAME;
  }

  @Nullable
  public String getTimeColumnName() {
    return DEFAULT_TIME_COLUMN_NAME;
  }

  public String getAvroTarFileName() {
    return DEFAULT_AVRO_TAR_FILE_NAME;
  }

  public List<File> getAndUnpackAllAvroFiles()
      throws Exception {
    // Unpack the Avro files
    int numSegments = unpackAvroData(_testCluster.getTempDir()).size();

    // Avro files has to be ordered as time series data
    List<File> avroFiles = new ArrayList<>(numSegments);
    for (int i = 1; i <= numSegments; i++) {
      avroFiles.add(new File(_testCluster.getTempDir(), "On_Time_On_Time_Performance_2014_" + i + ".avro"));
    }

    return avroFiles;
  }

  public long getCountStarResult() {
    return DEFAULT_COUNT_STAR_RESULT;
  }

  public String getStreamConsumerFactoryClassName() {
    return KafkaStarterUtils.KAFKA_STREAM_CONSUMER_FACTORY_CLASS_NAME;
  }

  public String getKafkaTopic() {
    return getClass().getSimpleName();
  }

  public int getMaxNumKafkaMessagesPerBatch() {
    return DEFAULT_MAX_NUM_KAFKA_MESSAGES_PER_BATCH;
  }

  @Nullable
  public byte[] getKafkaMessageHeader() {
    return null;
  }

  @Nullable
  public String getPartitionColumn() {
    return null;
  }

  @Nullable
  public String getSortedColumn() {
    return DEFAULT_SORTED_COLUMN;
  }

  @Nullable
  public List<String> getInvertedIndexColumns() {
    return DEFAULT_INVERTED_INDEX_COLUMNS;
  }

  @Nullable
  public List<String> getNoDictionaryColumns() {
    return DEFAULT_NO_DICTIONARY_COLUMNS;
  }

  @Nullable
  public List<String> getRangeIndexColumns() {
    return DEFAULT_RANGE_INDEX_COLUMNS;
  }

  @Nullable
  public List<String> getBloomFilterColumns() {
    return DEFAULT_BLOOM_FILTER_COLUMNS;
  }

  @Nullable
  public List<FieldConfig> getFieldConfigs() {
    return null;
  }

  public int getNumReplicas() {
    return DEFAULT_NUM_REPLICAS;
  }

  @Nullable
  public String getSegmentVersion() {
    return null;
  }

  @Nullable
  public String getLoadMode() {
    return null;
  }

  @Nullable
  public TableTaskConfig getTaskConfig() {
    return null;
  }

  @Nullable
  public String getBrokerTenant() {
    return TagNameUtils.DEFAULT_TENANT_NAME;
  }

  @Nullable
  public String getServerTenant() {
    return TagNameUtils.DEFAULT_TENANT_NAME;
  }

  @Nullable
  public IngestionConfig getIngestionConfig() {
    return null;
  }

  public QueryConfig getQueryconfig() {
    // Enable groovy for tables used in the tests
    return new QueryConfig(null, false, null, null);
  }

  public boolean getNullHandlingEnabled() {
    return DEFAULT_NULL_HANDLING_ENABLED;
  }

  @Nullable
  public SegmentPartitionConfig getSegmentPartitionConfig() {
    return null;
  }
}
