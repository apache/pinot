/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.integration.tests;

import com.google.common.base.Function;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.config.TableTaskConfig;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.ExternalView;
import org.testng.Assert;


/**
 * Shared implementation details of the cluster integration tests.
 */
public abstract class BaseClusterIntegrationTest extends ClusterTest {

  // Default settings
  private static final String DEFAULT_TABLE_NAME = "mytable";
  private static final String DEFAULT_SCHEMA_FILE_NAME = "On_Time_On_Time_Performance_2014_100k_subset_nonulls.schema";
  private static final String DEFAULT_AVRO_TAR_FILE_NAME =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls.tar.gz";
  private static final long DEFAULT_COUNT_STAR_RESULT = 115545L;
  private static final int DEFAULT_NUM_KAFKA_BROKERS = 1;
  private static final int DEFAULT_NUM_KAFKA_PARTITIONS = 10;
  private static final int DEFAULT_MAX_NUM_KAFKA_MESSAGES_PER_BATCH = 10000;
  private static final String DEFAULT_SORTED_COLUMN = "Carrier";
  private static final List<String> DEFAULT_INVERTED_INDEX_COLUMNS = Arrays.asList("FlightNum", "Origin", "Quarter");
  private static final List<String> DEFAULT_RAW_INDEX_COLUMNS =
      Arrays.asList("ActualElapsedTime", "ArrDelay", "DepDelay");

  protected final File _tempDir = new File(FileUtils.getTempDirectory(), getClass().getSimpleName());
  protected final File _segmentDir = new File(_tempDir, "segmentDir");
  protected final File _tarDir = new File(_tempDir, "tarDir");

  private Connection _h2Connection;
  private QueryGenerator _queryGenerator;

  /**
   * The following getters can be overridden to change default settings.
   */
  @Nonnull
  protected String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Nonnull
  protected String getSchemaFileName() {
    return DEFAULT_SCHEMA_FILE_NAME;
  }

  @Nonnull
  protected String getAvroTarFileName() {
    return DEFAULT_AVRO_TAR_FILE_NAME;
  }

  protected long getCountStarResult() {
    return DEFAULT_COUNT_STAR_RESULT;
  }

  @Nonnull
  protected String getKafkaTopic() {
    return getClass().getSimpleName();
  }

  protected int getNumKafkaBrokers() {
    return DEFAULT_NUM_KAFKA_BROKERS;
  }

  protected int getNumKafkaPartitions() {
    return DEFAULT_NUM_KAFKA_PARTITIONS;
  }

  protected int getMaxNumKafkaMessagesPerBatch() {
    return DEFAULT_MAX_NUM_KAFKA_MESSAGES_PER_BATCH;
  }

  @Nullable
  protected byte[] getKafkaMessageHeader() {
    return null;
  }

  @Nullable
  protected String getPartitionColumn() {
    return null;
  }

  @Nullable
  protected String getSortedColumn() {
    return DEFAULT_SORTED_COLUMN;
  }

  @Nullable
  protected List<String> getInvertedIndexColumns() {
    return DEFAULT_INVERTED_INDEX_COLUMNS;
  }

  @Nullable
  protected List<String> getRawIndexColumns() {
    return DEFAULT_RAW_INDEX_COLUMNS;
  }

  @Nullable
  protected String getLoadMode() {
    return null;
  }

  @Nullable
  protected TableTaskConfig getTaskConfig() {
    return null;
  }

  /**
   * Get the H2 connection. H2 connection must be set up before calling this method.
   *
   * @return H2 connection
   */
  @Nonnull
  protected Connection getH2Connection() {
    Assert.assertNotNull(_h2Connection, "H2 Connection has not been initialized");
    return _h2Connection;
  }

  /**
   * Get the query generator. Query generator must be set up before calling this method.
   *
   * @return Query generator.
   */
  @Nonnull
  protected QueryGenerator getQueryGenerator() {
    Assert.assertNotNull(_queryGenerator, "Query Generator has not been initialized");
    return _queryGenerator;
  }

  /**
   * Set up H2 connection to a table with pre-loaded data.
   *
   * @param avroFiles List of Avro files to be loaded.
   * @param executor Executor
   * @throws Exception
   */
  protected void setUpH2Connection(@Nonnull final List<File> avroFiles, @Nonnull ExecutorService executor)
      throws Exception {
    Assert.assertNull(_h2Connection);
    Class.forName("org.h2.Driver");
    _h2Connection = DriverManager.getConnection("jdbc:h2:mem:");
    executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          ClusterIntegrationTestUtils.setUpH2TableWithAvro(avroFiles, getTableName(), _h2Connection);
        } catch (Exception e) {
          // Ignored
        }
      }
    });
  }

  /**
   * Set up query generator using the given Avro files.
   *
   * @param avroFiles List of Avro files
   * @param executor Executor
   */
  protected void setupQueryGenerator(@Nonnull final List<File> avroFiles, @Nonnull ExecutorService executor) {
    Assert.assertNull(_queryGenerator);
    final String tableName = getTableName();
    executor.execute(new Runnable() {
      @Override
      public void run() {
        _queryGenerator = new QueryGenerator(avroFiles, tableName, tableName);
      }
    });
  }

  /**
   * Get the schema file.
   *
   * @return Schema file
   */
  @Nonnull
  protected File getSchemaFile() {
    URL resourceUrl = BaseClusterIntegrationTest.class.getClassLoader().getResource(getSchemaFileName());
    Assert.assertNotNull(resourceUrl);
    return new File(resourceUrl.getFile());
  }

  /**
   * Unpack the tarred Avro data into the given directory.
   *
   * @param outputDir Output directory
   * @return List of files unpacked.
   * @throws Exception
   */
  @Nonnull
  protected List<File> unpackAvroData(@Nonnull File outputDir)
      throws Exception {
    URL resourceUrl = BaseClusterIntegrationTest.class.getClassLoader().getResource(getAvroTarFileName());
    Assert.assertNotNull(resourceUrl);
    return TarGzCompressionUtils.unTar(new File(resourceUrl.getFile()), outputDir);
  }

  /**
   * Push the data in the given Avro files into a Kafka stream.
   *
   * @param avroFiles List of Avro files
   * @param kafkaTopic Kafka topic
   * @param executor Executor
   */
  protected void pushAvroIntoKafka(@Nonnull final List<File> avroFiles, @Nonnull final String kafkaTopic,
      @Nonnull ExecutorService executor) {
    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          ClusterIntegrationTestUtils.pushAvroIntoKafka(avroFiles, KafkaStarterUtils.DEFAULT_KAFKA_BROKER, kafkaTopic,
              getMaxNumKafkaMessagesPerBatch(), getKafkaMessageHeader(), getPartitionColumn());
        } catch (Exception e) {
          // Ignored
        }
      }
    });
  }

  /**
   * Get current result for "SELECT COUNT(*)".
   *
   * @return Current count start result
   * @throws Exception
   */
  protected long getCurrentCountStarResult()
      throws Exception {
    return postQuery("SELECT COUNT(*) FROM " + getTableName()).getJSONArray("aggregationResults")
        .getJSONObject(0)
        .getLong("value");
  }

  /**
   * Wait for all segments for the given table type to become online.
   *
   * @param tableType Table type
   * @param expectedNumSegments Expected number of online segments
   * @throws Exception
   */
  protected void waitForAllSegmentsOnline(@Nonnull CommonConstants.Helix.TableType tableType,
      final int expectedNumSegments, long timeoutMs)
      throws Exception {
    final String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(getTableName());
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          ExternalView externalView = _helixAdmin.getResourceExternalView(_clusterName, tableNameWithType);
          Set<String> partitionSet = externalView.getPartitionSet();
          if (partitionSet.size() == expectedNumSegments) {
            int numOnlinePartitions = 0;
            for (String partitionId : partitionSet) {
              Map<String, String> partitionStateMap = externalView.getStateMap(partitionId);
              if (partitionStateMap.containsValue("ONLINE")) {
                numOnlinePartitions++;
              }
            }
            if (numOnlinePartitions == expectedNumSegments) {
              return true;
            }
          }
          return false;
        } catch (Exception e) {
          return null;
        }
      }
    }, timeoutMs, "Failed to get all " + expectedNumSegments + " segments in table " + tableNameWithType + " online");
  }

  /**
   * Wait for all documents to get loaded.
   *
   * @param timeoutMs Timeout in milliseconds
   * @throws Exception
   */
  protected void waitForAllDocsLoaded(long timeoutMs)
      throws Exception {
    final long countStarResult = getCountStarResult();
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          return getCurrentCountStarResult() == countStarResult;
        } catch (Exception e) {
          return null;
        }
      }
    }, timeoutMs, "Failed to load " + countStarResult + " documents");
  }

  /**
   * Run equivalent Pinot and H2 query and compare the results.
   *
   * @param pqlQuery Pinot query
   * @param sqlQueries H2 query
   * @throws Exception
   */
  protected void testQuery(@Nonnull String pqlQuery, @Nullable List<String> sqlQueries)
      throws Exception {
    ClusterIntegrationTestUtils.testQuery(pqlQuery, BROKER_BASE_API_URL, sqlQueries, getH2Connection());
  }
}
