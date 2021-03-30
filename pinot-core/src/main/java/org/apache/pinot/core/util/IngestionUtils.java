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
package org.apache.pinot.core.util;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.core.data.function.FunctionEvaluator;
import org.apache.pinot.core.data.function.FunctionEvaluatorFactory;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.name.FixedSegmentNameGenerator;
import org.apache.pinot.core.segment.name.NormalizedDateSegmentNameGenerator;
import org.apache.pinot.core.segment.name.SegmentNameGenerator;
import org.apache.pinot.core.segment.name.SimpleSegmentNameGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.ingestion.batch.BatchConfig;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper methods for ingestion
 */
public final class IngestionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(IngestionUtils.class);

  private static final String DEFAULT_SEGMENT_NAME_GENERATOR_TYPE =
      BatchConfigProperties.SegmentNameGeneratorType.SIMPLE;
  private static final long DEFAULT_RETRY_WAIT_MS = 1000L;
  private static final int DEFAULT_ATTEMPTS = 3;
  private static final FileUploadDownloadClient FILE_UPLOAD_DOWNLOAD_CLIENT = new FileUploadDownloadClient();

  private IngestionUtils() {
  }

  /**
   * Create {@link SegmentGeneratorConfig} using tableConfig and schema.
   * All properties are taken from the 1st Map in tableConfig -> ingestionConfig -> batchIngestionConfig -> batchConfigMaps
   * @param tableConfig tableConfig with the batchConfigMap set
   * @param schema pinot schema
   */
  public static SegmentGeneratorConfig generateSegmentGeneratorConfig(TableConfig tableConfig, Schema schema)
      throws IOException, ClassNotFoundException {
    Preconditions.checkNotNull(tableConfig.getIngestionConfig(),
        "Must provide batchIngestionConfig in tableConfig for table: %s, for generating SegmentGeneratorConfig",
        tableConfig.getTableName());
    return generateSegmentGeneratorConfig(tableConfig, schema,
        tableConfig.getIngestionConfig().getBatchIngestionConfig());
  }

  /**
   * Create {@link SegmentGeneratorConfig} using tableConfig, schema and batchIngestionConfig.
   * The provided batchIngestionConfig will take precedence over the one in tableConfig
   */
  public static SegmentGeneratorConfig generateSegmentGeneratorConfig(TableConfig tableConfig, Schema schema,
      BatchIngestionConfig batchIngestionConfig)
      throws ClassNotFoundException, IOException {
    Preconditions.checkNotNull(batchIngestionConfig,
        "Must provide batchIngestionConfig in tableConfig for table: %s, for generating SegmentGeneratorConfig",
        tableConfig.getTableName());
    Preconditions.checkState(CollectionUtils.isNotEmpty(batchIngestionConfig.getBatchConfigMaps()),
        "Must provide batchConfigMap in tableConfig for table: %s, for generating SegmentGeneratorConfig",
        tableConfig.getTableName());
    BatchConfig batchConfig =
        new BatchConfig(tableConfig.getTableName(), batchIngestionConfig.getBatchConfigMaps().get(0));

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);

    // Input/output configs
    segmentGeneratorConfig.setInputFilePath(batchConfig.getInputDirURI());
    segmentGeneratorConfig.setOutDir(batchConfig.getOutputDirURI());

    // Reader configs
    segmentGeneratorConfig
        .setRecordReaderPath(RecordReaderFactory.getRecordReaderClassName(batchConfig.getInputFormat().toString()));
    Map<String, String> recordReaderProps = batchConfig.getRecordReaderProps();
    segmentGeneratorConfig.setReaderConfig(RecordReaderFactory.getRecordReaderConfig(batchConfig.getInputFormat(),
        IngestionConfigUtils.getRecordReaderProps(recordReaderProps)));

    // Segment name generator configs
    SegmentNameGenerator segmentNameGenerator =
        getSegmentNameGenerator(batchConfig, batchIngestionConfig.getSegmentIngestionType(),
            batchIngestionConfig.getSegmentIngestionFrequency(), tableConfig, schema);
    segmentGeneratorConfig.setSegmentNameGenerator(segmentNameGenerator);
    String sequenceId = batchConfig.getSequenceId();
    if (StringUtils.isNumeric(sequenceId)) {
      segmentGeneratorConfig.setSequenceId(Integer.parseInt(sequenceId));
    }

    return segmentGeneratorConfig;
  }

  private static SegmentNameGenerator getSegmentNameGenerator(BatchConfig batchConfig, String pushType,
      String pushFrequency, TableConfig tableConfig, Schema schema) {

    String rawTableName = TableNameBuilder.extractRawTableName(batchConfig.getTableNameWithType());
    String segmentNameGeneratorType = batchConfig.getSegmentNameGeneratorType();
    if (segmentNameGeneratorType == null) {
      segmentNameGeneratorType = DEFAULT_SEGMENT_NAME_GENERATOR_TYPE;
    }
    switch (segmentNameGeneratorType) {
      case BatchConfigProperties.SegmentNameGeneratorType.FIXED:
        return new FixedSegmentNameGenerator(batchConfig.getSegmentName());

      case BatchConfigProperties.SegmentNameGeneratorType.NORMALIZED_DATE:
        DateTimeFormatSpec dateTimeFormatSpec = null;
        String timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
        if (timeColumnName != null) {
          DateTimeFieldSpec dateTimeFieldSpec = schema.getSpecForTimeColumn(timeColumnName);
          if (dateTimeFieldSpec != null) {
            dateTimeFormatSpec = new DateTimeFormatSpec(dateTimeFieldSpec.getFormat());
          }
        }
        return new NormalizedDateSegmentNameGenerator(rawTableName, batchConfig.getSegmentNamePrefix(),
            batchConfig.isExcludeSequenceId(), pushType, pushFrequency, dateTimeFormatSpec);

      case BatchConfigProperties.SegmentNameGeneratorType.SIMPLE:
        return new SimpleSegmentNameGenerator(rawTableName, batchConfig.getSegmentNamePostfix());

      default:
        throw new IllegalStateException(String
            .format("Unsupported segmentNameGeneratorType: %s for table: %s", segmentNameGeneratorType,
                tableConfig.getTableName()));
    }
  }

  /**
   * Builds a segment using given {@link SegmentGeneratorConfig}
   * @return segment name
   */
  public static String buildSegment(SegmentGeneratorConfig segmentGeneratorConfig)
      throws Exception {
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();
    return driver.getSegmentName();
  }

  /**
   * Uploads the segment tar files to the provided controller
   */
  public static void uploadSegment(String tableNameWithType, List<File> tarFiles, URI controllerUri,
      final String authToken)
      throws RetriableOperationException, AttemptsExceededException {
    for (File tarFile : tarFiles) {
      String fileName = tarFile.getName();
      Preconditions.checkArgument(fileName.endsWith(Constants.TAR_GZ_FILE_EXT));
      String segmentName = fileName.substring(0, fileName.length() - Constants.TAR_GZ_FILE_EXT.length());

      RetryPolicies.exponentialBackoffRetryPolicy(DEFAULT_ATTEMPTS, DEFAULT_RETRY_WAIT_MS, 5).attempt(() -> {
        try (InputStream inputStream = new FileInputStream(tarFile)) {
          SimpleHttpResponse response = FILE_UPLOAD_DOWNLOAD_CLIENT
              .uploadSegment(FileUploadDownloadClient.getUploadSegmentURI(controllerUri), segmentName, inputStream,
                  FileUploadDownloadClient.makeAuthHeader(authToken),
                  FileUploadDownloadClient.makeTableParam(tableNameWithType),
                  FileUploadDownloadClient.DEFAULT_SOCKET_TIMEOUT_MS);
          LOGGER.info("Response for pushing table {} segment {} - {}: {}", tableNameWithType, segmentName,
              response.getStatusCode(), response.getResponse());
          return true;
        } catch (HttpErrorStatusException e) {
          int statusCode = e.getStatusCode();
          if (statusCode >= 500) {
            LOGGER.warn("Caught temporary exception while pushing table: {} segment: {}, will retry", tableNameWithType,
                segmentName, e);
            return false;
          } else {
            throw e;
          }
        }
      });
    }
  }

  /**
   * Extracts all fields required by the {@link org.apache.pinot.spi.data.readers.RecordExtractor} from the given TableConfig and Schema
   * Fields for ingestion come from 2 places:
   * 1. The schema
   * 2. The ingestion config in the table config. The ingestion config (e.g. filter) can have fields which are not in the schema.
   */
  public static Set<String> getFieldsForRecordExtractor(@Nullable IngestionConfig ingestionConfig, Schema schema) {
    Set<String> fieldsForRecordExtractor = new HashSet<>();
    extractFieldsFromIngestionConfig(ingestionConfig, fieldsForRecordExtractor);
    extractFieldsFromSchema(schema, fieldsForRecordExtractor);
    return fieldsForRecordExtractor;
  }

  /**
   * Extracts all the fields needed by the {@link org.apache.pinot.spi.data.readers.RecordExtractor} from the given Schema
   * TODO: for now, we assume that arguments to transform function are in the source i.e. no columns are derived from transformed columns
   */
  private static void extractFieldsFromSchema(Schema schema, Set<String> fields) {
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn()) {
        FunctionEvaluator functionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(fieldSpec);
        if (functionEvaluator != null) {
          fields.addAll(functionEvaluator.getArguments());
        }
        fields.add(fieldSpec.getName());
      }
    }
  }

  /**
   * Extracts the fields needed by a RecordExtractor from given {@link IngestionConfig}
   */
  private static void extractFieldsFromIngestionConfig(@Nullable IngestionConfig ingestionConfig, Set<String> fields) {
    if (ingestionConfig != null) {
      FilterConfig filterConfig = ingestionConfig.getFilterConfig();
      if (filterConfig != null) {
        String filterFunction = filterConfig.getFilterFunction();
        if (filterFunction != null) {
          FunctionEvaluator functionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(filterFunction);
          fields.addAll(functionEvaluator.getArguments());
        }
      }
      List<TransformConfig> transformConfigs = ingestionConfig.getTransformConfigs();
      if (transformConfigs != null) {
        for (TransformConfig transformConfig : transformConfigs) {
          FunctionEvaluator expressionEvaluator =
              FunctionEvaluatorFactory.getExpressionEvaluator(transformConfig.getTransformFunction());
          fields.addAll(expressionEvaluator.getArguments());
          fields.add(transformConfig
              .getColumnName()); // add the column itself too, so that if it is already transformed, we won't transform again
        }
      }
    }
  }

  /**
   * Returns false if the record contains key {@link GenericRow#SKIP_RECORD_KEY} with value true
   */
  public static boolean shouldIngestRow(GenericRow genericRow) {
    return !Boolean.TRUE.equals(genericRow.getValue(GenericRow.SKIP_RECORD_KEY));
  }

  public static Long extractTimeValue(Comparable time) {
    if (time != null) {
      if (time instanceof Number) {
        return ((Number) time).longValue();
      } else {
        String stringValue = time.toString();
        if (StringUtils.isNumeric(stringValue)) {
          return Long.parseLong(stringValue);
        }
      }
    }
    return null;
  }
}
