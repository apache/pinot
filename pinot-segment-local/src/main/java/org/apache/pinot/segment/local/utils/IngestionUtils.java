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
package org.apache.pinot.segment.local.utils;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.segment.local.function.FunctionEvaluator;
import org.apache.pinot.segment.local.function.FunctionEvaluatorFactory;
import org.apache.pinot.segment.local.recordtransformer.ComplexTypeTransformer;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.name.FixedSegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.NormalizedDateSegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.SegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.SimpleSegmentNameGenerator;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.BatchConfig;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;


/**
 * Helper methods for ingestion
 */
public final class IngestionUtils {

  private static final PinotFS LOCAL_PINOT_FS = new LocalPinotFS();

  private IngestionUtils() {
  }

  /**
   * Create {@link SegmentGeneratorConfig} using tableConfig and schema.
   * All properties are taken from the 1st Map in tableConfig -> ingestionConfig -> batchIngestionConfig ->
   * batchConfigMaps
   * @param tableConfig tableConfig with the batchConfigMap set
   * @param schema pinot schema
   */
  public static SegmentGeneratorConfig generateSegmentGeneratorConfig(TableConfig tableConfig, Schema schema)
      throws IOException, ClassNotFoundException {
    Preconditions.checkNotNull(tableConfig.getIngestionConfig(),
        "Must provide ingestionConfig in tableConfig for table: %s, for generating SegmentGeneratorConfig",
        tableConfig.getTableName());
    Preconditions.checkNotNull(tableConfig.getIngestionConfig().getBatchIngestionConfig(),
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
    Preconditions.checkState(batchIngestionConfig != null && batchIngestionConfig.getBatchConfigMaps() != null
        && batchIngestionConfig.getBatchConfigMaps().size() == 1,
        "Must provide batchIngestionConfig and contains exactly 1 batchConfigMap for table: %s, "
            + "for generating SegmentGeneratorConfig",
        tableConfig.getTableName());

    // apply config override provided by user.
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
    switch (segmentNameGeneratorType) {
      case BatchConfigProperties.SegmentNameGeneratorType.FIXED:
        return new FixedSegmentNameGenerator(batchConfig.getSegmentName());

      case BatchConfigProperties.SegmentNameGeneratorType.NORMALIZED_DATE:
        DateTimeFormatSpec dateTimeFormatSpec = null;
        String timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
        if (timeColumnName != null) {
          DateTimeFieldSpec dateTimeFieldSpec = schema.getSpecForTimeColumn(timeColumnName);
          if (dateTimeFieldSpec != null) {
            dateTimeFormatSpec = dateTimeFieldSpec.getFormatSpec();
          }
        }
        return new NormalizedDateSegmentNameGenerator(rawTableName, batchConfig.getSegmentNamePrefix(),
            batchConfig.isExcludeSequenceId(), pushType, pushFrequency, dateTimeFormatSpec,
            batchConfig.getSegmentNamePostfix(), batchConfig.isAppendUUIDToSegmentName());

      case BatchConfigProperties.SegmentNameGeneratorType.SIMPLE:
        return new SimpleSegmentNameGenerator(rawTableName, batchConfig.getSegmentNamePostfix(),
            batchConfig.isAppendUUIDToSegmentName());
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
   * Uploads the segments from the provided segmentTar URIs to the table, using push details from the batchConfig
   * @param tableNameWithType name of the table to upload the segment
   * @param batchConfig batchConfig with details about push such as controllerURI, pushAttempts, pushParallelism, etc
   * @param segmentTarURIs list of URI for the segment tar files
   * @param authProvider auth provider
   */
  public static void uploadSegment(String tableNameWithType, BatchConfig batchConfig, List<URI> segmentTarURIs,
      @Nullable AuthProvider authProvider)
      throws Exception {

    SegmentGenerationJobSpec segmentUploadSpec = generateSegmentUploadSpec(tableNameWithType, batchConfig,
        authProvider);

    List<String> segmentTarURIStrs = segmentTarURIs.stream().map(URI::toString).collect(Collectors.toList());
    String pushMode = batchConfig.getPushMode();
    switch (BatchConfigProperties.SegmentPushType.valueOf(pushMode.toUpperCase())) {
      case TAR:
        try {
          SegmentPushUtils.pushSegments(segmentUploadSpec, LOCAL_PINOT_FS, segmentTarURIStrs);
        } catch (RetriableOperationException | AttemptsExceededException e) {
          throw new RuntimeException(String
              .format("Caught exception while uploading segments. Push mode: TAR, segment tars: [%s]",
                  segmentTarURIStrs), e);
        }
        break;
      case URI:
        List<String> segmentUris = new ArrayList<>();
        try {
          URI outputSegmentDirURI = null;
          if (StringUtils.isNotBlank(batchConfig.getOutputSegmentDirURI())) {
            outputSegmentDirURI = URI.create(batchConfig.getOutputSegmentDirURI());
          }
          for (URI segmentTarURI : segmentTarURIs) {
            URI updatedURI = SegmentPushUtils.generateSegmentTarURI(outputSegmentDirURI, segmentTarURI,
                segmentUploadSpec.getPushJobSpec().getSegmentUriPrefix(),
                segmentUploadSpec.getPushJobSpec().getSegmentUriSuffix());
            segmentUris.add(updatedURI.toString());
          }
          SegmentPushUtils.sendSegmentUris(segmentUploadSpec, segmentUris);
        } catch (RetriableOperationException | AttemptsExceededException e) {
          throw new RuntimeException(String
              .format("Caught exception while uploading segments. Push mode: URI, segment URIs: [%s]", segmentUris), e);
        }
        break;
      case METADATA:
        try {
          URI outputSegmentDirURI = null;
          if (StringUtils.isNotBlank(batchConfig.getOutputSegmentDirURI())) {
            outputSegmentDirURI = URI.create(batchConfig.getOutputSegmentDirURI());
          }
          PinotFS outputFileFS = getOutputPinotFS(batchConfig, outputSegmentDirURI);
          Map<String, String> segmentUriToTarPathMap = SegmentPushUtils.getSegmentUriToTarPathMap(outputSegmentDirURI,
              segmentUploadSpec.getPushJobSpec(), segmentTarURIStrs.toArray(new String[0]));
          SegmentPushUtils.sendSegmentUriAndMetadata(segmentUploadSpec, outputFileFS, segmentUriToTarPathMap);
        } catch (RetriableOperationException | AttemptsExceededException e) {
          throw new RuntimeException(String
              .format("Caught exception while uploading segments. Push mode: METADATA, segment URIs: [%s]",
                  segmentTarURIStrs), e);
        }
        break;
      default:
        throw new UnsupportedOperationException("Unrecognized push mode - " + pushMode);
    }
  }

  private static SegmentGenerationJobSpec generateSegmentUploadSpec(String tableName, BatchConfig batchConfig,
      @Nullable AuthProvider authProvider) {

    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName(tableName);

    PinotClusterSpec pinotClusterSpec = new PinotClusterSpec();
    pinotClusterSpec.setControllerURI(batchConfig.getPushControllerURI());
    PinotClusterSpec[] pinotClusterSpecs = new PinotClusterSpec[]{pinotClusterSpec};

    PushJobSpec pushJobSpec = new PushJobSpec();
    pushJobSpec.setPushAttempts(batchConfig.getPushAttempts());
    pushJobSpec.setPushParallelism(batchConfig.getPushParallelism());
    pushJobSpec.setPushRetryIntervalMillis(batchConfig.getPushIntervalRetryMillis());
    pushJobSpec.setSegmentUriPrefix(batchConfig.getPushSegmentURIPrefix());
    pushJobSpec.setSegmentUriSuffix(batchConfig.getPushSegmentURISuffix());

    SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();
    spec.setPushJobSpec(pushJobSpec);
    spec.setTableSpec(tableSpec);
    spec.setPinotClusterSpecs(pinotClusterSpecs);
    spec.setAuthToken(AuthProviderUtils.toStaticToken(authProvider));

    return spec;
  }

  /**
   * Creates an instance of the PinotFS using the fileURI and fs properties from BatchConfig
   */
  public static PinotFS getOutputPinotFS(BatchConfig batchConfig, URI fileURI) {
    String fileURIScheme = (fileURI == null) ? null : fileURI.getScheme();
    if (fileURIScheme == null) {
      fileURIScheme = PinotFSFactory.LOCAL_PINOT_FS_SCHEME;
    }
    if (!PinotFSFactory.isSchemeSupported(fileURIScheme)) {
      registerPinotFS(fileURIScheme, batchConfig.getOutputFsClassName(),
          IngestionConfigUtils.getOutputFsProps(batchConfig.getBatchConfigMap()));
    }
    return PinotFSFactory.create(fileURIScheme);
  }

  private static void registerPinotFS(String fileURIScheme, String fsClass, PinotConfiguration fsProps) {
    PinotFSFactory.register(fileURIScheme, fsClass, fsProps);
  }

  /**
   * Extracts all fields required by the {@link org.apache.pinot.spi.data.readers.RecordExtractor} from the given
   * TableConfig and Schema
   * Fields for ingestion come from 2 places:
   * 1. The schema
   * 2. The ingestion config in the table config. The ingestion config (e.g. filter, complexType) can have fields which
   * are not in the schema.
   */
  public static Set<String> getFieldsForRecordExtractor(@Nullable IngestionConfig ingestionConfig, Schema schema) {
    Set<String> fieldsForRecordExtractor = new HashSet<>();
    extractFieldsFromIngestionConfig(ingestionConfig, fieldsForRecordExtractor);
    extractFieldsFromSchema(schema, fieldsForRecordExtractor);
    fieldsForRecordExtractor = getFieldsToReadWithComplexType(fieldsForRecordExtractor, ingestionConfig);
    return fieldsForRecordExtractor;
  }

  /**
   * Extracts the root-level names from the fields, to support the complex-type handling. For example,
   * a field a.b.c will return the top-level name a.
   */
  private static Set<String> getFieldsToReadWithComplexType(Set<String> fieldsToRead, IngestionConfig ingestionConfig) {
    if (ingestionConfig == null || ingestionConfig.getComplexTypeConfig() == null) {
      // do nothing
      return fieldsToRead;
    }
    ComplexTypeConfig complexTypeConfig = ingestionConfig.getComplexTypeConfig();
    Set<String> result = new HashSet<>();
    String delimiter = complexTypeConfig.getDelimiter() == null ? ComplexTypeTransformer.DEFAULT_DELIMITER
        : complexTypeConfig.getDelimiter();
    for (String field : fieldsToRead) {
      result.add(StringUtils.splitByWholeSeparator(field, delimiter)[0]);
    }
    return result;
  }

  /**
   * Extracts all the fields needed by the {@link org.apache.pinot.spi.data.readers.RecordExtractor} from the given
   * Schema
   * TODO: for now, we assume that arguments to transform function are in the source i.e. no columns are derived from
   * transformed columns
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
      List<AggregationConfig> aggregationConfigs = ingestionConfig.getAggregationConfigs();
      if (aggregationConfigs != null) {
        for (AggregationConfig aggregationConfig : aggregationConfigs) {
          ExpressionContext expressionContext =
              RequestContextUtils.getExpression(aggregationConfig.getAggregationFunction());
          expressionContext.getColumns(fields);
        }
      }
      List<TransformConfig> transformConfigs = ingestionConfig.getTransformConfigs();
      if (transformConfigs != null) {
        for (TransformConfig transformConfig : transformConfigs) {
          FunctionEvaluator expressionEvaluator =
              FunctionEvaluatorFactory.getExpressionEvaluator(transformConfig.getTransformFunction());
          fields.addAll(expressionEvaluator.getArguments());
          fields.add(transformConfig
              .getColumnName()); // add the column itself too, so that if it is already transformed, we won't
          // transform again
        }
      }
      ComplexTypeConfig complexTypeConfig = ingestionConfig.getComplexTypeConfig();
      if (complexTypeConfig != null && complexTypeConfig.getFieldsToUnnest() != null) {
        fields.addAll(complexTypeConfig.getFieldsToUnnest());
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
