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
package org.apache.pinot.plugin.minion.tasks.refreshsegment;

import java.io.File;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.pinot.common.evaluator.FunctionEvaluatorFactory;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.plugin.minion.tasks.BaseSingleSegmentConversionExecutor;
import org.apache.pinot.plugin.minion.tasks.MinionTaskUtils;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformerUtils;
import org.apache.pinot.segment.local.recordtransformer.TransformProvenanceUtils;
import org.apache.pinot.segment.local.segment.creator.RecordReaderSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.creator.impl.BaseSegmentCreator;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.local.utils.NullValueTransformerUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.function.FunctionEvaluator;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.Obfuscator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RefreshSegmentTaskExecutor extends BaseSingleSegmentConversionExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(RefreshSegmentTaskExecutor.class);

  private long _taskStartTime;

  /// The code here currently covers segment refresh for the following cases:
  /// 1. Process newly added columns.
  /// 2. Addition/removal of indexes.
  /// 3. Compatible datatype change for existing columns
  @Override
  protected SegmentConversionResult convert(PinotTaskConfig pinotTaskConfig, File indexDir, File workingDir)
      throws Exception {
    _eventObserver.notifyProgress(pinotTaskConfig, "Refreshing segment: " + indexDir);

    // We set _taskStartTime before fetching the tableConfig. Task Generation relies on tableConfig/Schema updates
    // happening after the last processed time. So we explicitly use the timestamp before fetching tableConfig as the
    // processedTime.
    _taskStartTime = System.currentTimeMillis();
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    String segmentName = configs.get(MinionConstants.SEGMENT_NAME_KEY);
    String taskType = pinotTaskConfig.getTaskType();

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Starting task: {} with configs: {}", taskType, Obfuscator.DEFAULT.toJsonString(configs));
    }

    TableConfig tableConfig = getTableConfig(tableNameWithType);
    Schema schema = getSchema(tableNameWithType);

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    SegmentDirectoryLoaderContext segmentLoaderContext = new SegmentDirectoryLoaderContext.Builder()
        .setReadMode(indexLoadingConfig.getReadMode())
        .setTableConfig(indexLoadingConfig.getTableConfig())
        .setSchema(schema)
        .setInstanceId(indexLoadingConfig.getInstanceId())
        .setSegmentName(segmentMetadata.getName())
        .setSegmentCrc(segmentMetadata.getCrc())
        .build();
    // TODO: Instead of relying on needPreprocess(), process segment metadata file to determine if refresh is needed.
    // BaseDefaultColumnHandler part of needPreprocess() does not process any changes to existing columns like datatype,
    // change from dimension to metric, etc.
    Map<String, String> transformFunctionByColumn =
        IngestionConfigUtils.getTransformFunctionByColumn(tableConfig, schema);
    Map<String, String> transformFingerprintByColumn =
        TransformProvenanceUtils.getTransformFingerprints(tableConfig, schema);
    Set<String> changedTransformColumns = getColumnsWithChangedTransformValues(schema, segmentMetadata,
        transformFunctionByColumn, transformFingerprintByColumn);
    Set<String> trustedTransformColumns = getColumnsWithTrustedTransformValues(schema, segmentMetadata,
        transformFunctionByColumn, transformFingerprintByColumn);
    Map<String, ColumnMetadata> originalColumnMetadata = new HashMap<>();
    for (ColumnMetadata columnMetadata : segmentMetadata.getColumnMetadataMap().values()) {
      if (schema.hasColumn(columnMetadata.getColumnName())) {
        originalColumnMetadata.put(columnMetadata.getColumnName(), columnMetadata);
      }
    }
    SegmentDirectory segmentDirectory =
        SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader().load(indexDir.toURI(), segmentLoaderContext);
    boolean needPreprocess;
    try {
      needPreprocess = ImmutableSegmentLoader.needPreprocess(segmentDirectory, indexLoadingConfig);
    } finally {
      closeSegmentDirectoryQuietly(segmentDirectory);
    }
    Set<String> refreshColumnSet = new HashSet<>();

    for (FieldSpec fieldSpecInSchema : schema.getAllFieldSpecs()) {
      // Virtual columns are constructed while loading the segment, thus do not exist in the record, nor should be
      // persisted to the disk.
      if (fieldSpecInSchema.isVirtualColumn()) {
        continue;
      }

      String column = fieldSpecInSchema.getName();
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null) {
        FieldSpec fieldSpecInSegment = columnMetadata.getFieldSpec();

        // Any structural difference must force record replay. The default-column handler deliberately defers
        // transform-chain participants, so needPreprocess() alone is not a sufficient replay signal for them.
        if (fieldSpecInSegment.getFieldType() != fieldSpecInSchema.getFieldType()
            || fieldSpecInSegment.getDataType() != fieldSpecInSchema.getDataType()
            || fieldSpecInSegment.isSingleValueField() != fieldSpecInSchema.isSingleValueField()
            || !Objects.equals(fieldSpecInSegment.getDefaultNullValueString(),
                fieldSpecInSchema.getDefaultNullValueString())) {
          refreshColumnSet.add(column);
        }
      } else {
        refreshColumnSet.add(column);
      }
    }
    // A chain member removed from the schema is also deferred by the default-column handler. Keep the removed column
    // as a replay root so its stored value is not carried into the rebuilt segment or its current dependents.
    for (ColumnMetadata columnMetadata : segmentMetadata.getColumnMetadataMap().values()) {
      if (columnMetadata.isAutoGenerated() && !schema.hasColumn(columnMetadata.getColumnName())) {
        refreshColumnSet.add(columnMetadata.getColumnName());
      }
    }

    Set<String> transformColumnsToRecompute = getTransformColumnsToRecompute(changedTransformColumns,
        refreshColumnSet, transformFunctionByColumn, transformFingerprintByColumn, segmentMetadata);
    Map<String, Object> replayDefaultValues = getReplayDefaultValues(refreshColumnSet, transformFunctionByColumn,
        tableConfig, schema, segmentMetadata);
    trustedTransformColumns.removeAll(transformColumnsToRecompute);

    if (!needPreprocess && refreshColumnSet.isEmpty() && changedTransformColumns.isEmpty()) {
      LOGGER.info("Skipping segment={}, table={} as it is up-to-date with new table/schema", segmentName,
          tableNameWithType);
      // We just need to update the ZK metadata with the last refresh time to avoid getting picked up again. As the CRC
      // check will match, this will only end up being a ZK update.
      return new SegmentConversionResult.Builder().setTableNameWithType(tableNameWithType)
          .setFile(indexDir)
          .setSegmentName(segmentName)
          .build();
    }

    // Refresh the segment. Segment reload is achieved by generating a new segment from scratch using the updated schema
    // and table configs.
    // Load with the table-config-derived IndexLoadingConfig so column readers configured via the table config are
    // honored (needPreprocess=false: read-only).
    ImmutableSegment segment = ImmutableSegmentLoader.load(indexDir, indexLoadingConfig, false);
    try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
      Set<String> fieldsToRead = new HashSet<>(segment.getPhysicalColumnNames());
      fieldsToRead.removeAll(transformColumnsToRecompute);
      recordReader.initWithFieldsToRead(segment, fieldsToRead);
      SegmentGeneratorConfig config =
          getSegmentGeneratorConfig(workingDir, tableConfig, segmentMetadata, segmentName, schema);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      List<RecordTransformer> recordTransformers = new ArrayList<>();
      if (!replayDefaultValues.isEmpty()) {
        recordTransformers.add(new ReplayDefaultValueTransformer(replayDefaultValues));
      }
      recordTransformers.addAll(RecordTransformerUtils.getDefaultTransformers(tableConfig, schema, fieldsToRead,
          trustedTransformColumns));
      driver.init(config, new RecordReaderSegmentCreationDataSource(recordReader),
          new TransformPipeline(tableConfig.getTableName(), recordTransformers));
      driver.build();
      preserveOriginalColumnMetadata(new File(workingDir, segmentName), originalColumnMetadata,
          transformColumnsToRecompute, changedTransformColumns, transformFunctionByColumn,
          replayDefaultValues.keySet());
      _eventObserver.notifyProgress(pinotTaskConfig,
          "Segment processing stats - incomplete rows:" + driver.getIncompleteRowsFound() + ", dropped rows:"
              + driver.getSkippedRowsFound() + ", sanitized rows:" + driver.getSanitizedRowsFound());
    } finally {
      segment.destroy();
    }

    File refreshedSegmentFile = new File(workingDir, segmentName);
    SegmentConversionResult result = new SegmentConversionResult.Builder().setFile(refreshedSegmentFile)
        .setTableNameWithType(tableNameWithType)
        .setSegmentName(segmentName)
        .build();

    long endMillis = System.currentTimeMillis();
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Finished task: {} with configs: {}. Total time: {}ms", taskType,
          Obfuscator.DEFAULT.toJsonString(configs), (endMillis - _taskStartTime));
    }

    return result;
  }

  private static Set<String> getColumnsWithChangedTransformValues(Schema schema, SegmentMetadataImpl segmentMetadata,
      Map<String, String> transformFunctionByColumn, Map<String, String> transformFingerprintByColumn) {
    Set<String> changedColumns = new HashSet<>();
    for (ColumnMetadata columnMetadata : segmentMetadata.getColumnMetadataMap().values()) {
      String column = columnMetadata.getColumnName();
      // Legacy columns have no provenance and must remain untouched. For known provenance, refresh must include
      // ordinary ingestion-derived columns as well as auto-generated default columns because both are rebuilt by
      // record replay.
      if (schema.hasColumn(column) && TransformProvenanceUtils.hasTransformChanged(columnMetadata,
          transformFunctionByColumn.get(column), transformFingerprintByColumn.get(column))) {
        changedColumns.add(column);
      }
    }
    return changedColumns;
  }

  private static Set<String> getColumnsWithTrustedTransformValues(Schema schema, SegmentMetadataImpl segmentMetadata,
      Map<String, String> transformFunctionByColumn, Map<String, String> transformFingerprintByColumn) {
    Set<String> trustedColumns = new HashSet<>();
    for (ColumnMetadata columnMetadata : segmentMetadata.getColumnMetadataMap().values()) {
      String column = columnMetadata.getColumnName();
      if (schema.hasColumn(column) && TransformProvenanceUtils.hasCurrentDependencyClosedProvenance(columnMetadata,
          transformFunctionByColumn.get(column), transformFingerprintByColumn.get(column))) {
        trustedColumns.add(column);
      }
    }
    return trustedColumns;
  }

  private static Set<String> getTransformColumnsToRecompute(Set<String> changedTransformColumns,
      Set<String> refreshColumnSet, Map<String, String> transformFunctionByColumn,
      Map<String, String> transformFingerprintByColumn, SegmentMetadataImpl segmentMetadata) {
    Set<String> recomputeRoots = new HashSet<>(changedTransformColumns);
    Set<String> changedInputs = new HashSet<>(changedTransformColumns);
    for (String column : refreshColumnSet) {
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      String transformFunction = transformFunctionByColumn.get(column);
      // Raw columns remain authoritative inputs during replay. A current transform output must instead be omitted so
      // the pipeline can regenerate it, unless it is an existing legacy column whose origin is unknown. Missing
      // transform outputs are always safe to generate from the active config.
      if (transformFunction == null) {
        changedInputs.add(column);
        if (columnMetadata != null && columnMetadata.isAutoGenerated()) {
          // A mutable default column must be regenerated from the active schema before its dependent transforms run.
          // Genuine source columns remain authoritative and stay in fieldsToRead.
          recomputeRoots.add(column);
        }
      } else if (columnMetadata == null
          || columnMetadata.getTransformFunctionProvenanceVersion() != ColumnMetadata.UNAVAILABLE) {
        changedInputs.add(column);
        recomputeRoots.add(column);
      }
    }
    Set<String> affectedOutputs = getAffectedTransformOutputs(recomputeRoots, changedInputs,
        transformFunctionByColumn, segmentMetadata);
    Set<String> dependencyClosure =
        TransformProvenanceUtils.getTransformDependencyClosure(affectedOutputs, transformFunctionByColumn);
    Set<String> columnsToRecompute = new HashSet<>(affectedOutputs);
    for (String column : dependencyClosure) {
      if (columnsToRecompute.contains(column)) {
        continue;
      }
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      // Non-persisted intermediates are naturally recomputed. Persisted ancestors are safe to omit only when their
      // metadata says every stored value was generated by the same direct expression. Legacy/source-mixed values stay
      // authoritative because their origin is unknown.
      if (columnMetadata == null || TransformProvenanceUtils.hasCurrentDependencyClosedProvenance(columnMetadata,
          transformFunctionByColumn.get(column), transformFingerprintByColumn.get(column))) {
        columnsToRecompute.add(column);
      }
    }
    return columnsToRecompute;
  }

  private static Map<String, Object> getReplayDefaultValues(Set<String> refreshColumnSet,
      Map<String, String> transformFunctionByColumn, TableConfig tableConfig, Schema schema,
      SegmentMetadataImpl segmentMetadata) {
    Map<String, Object> replayDefaultValues = new HashMap<>();
    for (String column : refreshColumnSet) {
      FieldSpec fieldSpec = schema.getFieldSpecFor(column);
      if (fieldSpec == null || transformFunctionByColumn.containsKey(column)) {
        continue;
      }
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata == null || columnMetadata.isAutoGenerated()) {
        replayDefaultValues.put(column,
            NullValueTransformerUtils.getDefaultNullValue(fieldSpec, tableConfig, schema));
      }
    }
    return replayDefaultValues;
  }

  private static Set<String> getAffectedTransformOutputs(Set<String> recomputeRoots, Set<String> changedInputs,
      Map<String, String> transformFunctionByColumn, SegmentMetadataImpl segmentMetadata) {
    Map<String, Set<String>> dependentsByColumn = new HashMap<>();
    for (Map.Entry<String, String> entry : transformFunctionByColumn.entrySet()) {
      FunctionEvaluator evaluator = FunctionEvaluatorFactory.getExpressionEvaluator(entry.getValue());
      for (String argument : evaluator.getArguments()) {
        dependentsByColumn.computeIfAbsent(argument, ignored -> new HashSet<>()).add(entry.getKey());
      }
    }

    Set<String> affectedOutputs = new HashSet<>(recomputeRoots);
    ArrayDeque<String> pending = new ArrayDeque<>(changedInputs);
    while (!pending.isEmpty()) {
      for (String dependent : dependentsByColumn.getOrDefault(pending.removeFirst(), Set.of())) {
        ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(dependent);
        // A persisted legacy output is an authoritative barrier because its values may have come from the source.
        // Known-provenance outputs and non-persisted intermediates must follow an upstream value change.
        if ((columnMetadata == null
            || columnMetadata.getTransformFunctionProvenanceVersion() != ColumnMetadata.UNAVAILABLE)
            && affectedOutputs.add(dependent)) {
          pending.addLast(dependent);
        }
      }
    }
    return affectedOutputs;
  }

  private static SegmentGeneratorConfig getSegmentGeneratorConfig(File workingDir, TableConfig tableConfig,
      SegmentMetadataImpl segmentMetadata, String segmentName, Schema schema) {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setInstanceType(InstanceType.MINION);
    config.setOutDir(workingDir.getPath());
    config.setSegmentName(segmentName);

    // Keep index creation time the same as original segment because both segments use the same raw data.
    // This way, for REFRESH case, when new segment gets pushed to controller, we can use index creation time to
    // identify if the new pushed segment has newer data than the existing one.
    config.setCreationTime(String.valueOf(segmentMetadata.getIndexCreationTime()));

    // The time column type info is not stored in the segment metadata.
    // Keep segment start/end time to properly handle time column type other than EPOCH (e.g.SIMPLE_FORMAT).
    if (segmentMetadata.getTimeInterval() != null) {
      config.setTimeColumnName(tableConfig.getValidationConfig().getTimeColumnName());
      config.setStartTime(Long.toString(segmentMetadata.getStartTime()));
      config.setEndTime(Long.toString(segmentMetadata.getEndTime()));
      config.setSegmentTimeUnit(segmentMetadata.getTimeUnit());
    }
    return config;
  }

  private static void preserveOriginalColumnMetadata(File refreshedSegmentDir,
      Map<String, ColumnMetadata> originalMetadata, Set<String> transformColumnsToRecompute,
      Set<String> changedTransformColumns, Map<String, String> transformFunctionByColumn,
      Set<String> replayDefaultColumns)
      throws Exception {
    if (originalMetadata.isEmpty() && replayDefaultColumns.isEmpty()) {
      return;
    }
    var properties = SegmentMetadataUtils.getPropertiesConfiguration(refreshedSegmentDir);
    for (Map.Entry<String, ColumnMetadata> entry : originalMetadata.entrySet()) {
      String column = entry.getKey();
      ColumnMetadata columnMetadata = entry.getValue();
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column,
          V1Constants.MetadataKeys.Column.IS_AUTO_GENERATED), columnMetadata.isAutoGenerated());
      if (changedTransformColumns.contains(column)) {
        // A removed transform is deliberately regenerated as schema default values. Record that known no-transform
        // state so adding an expression later is distinguishable from legacy metadata with unknown provenance.
        if (transformFunctionByColumn.get(column) == null) {
          BaseSegmentCreator.addTransformFunction(properties, column, null);
        }
      } else if (!transformColumnsToRecompute.contains(column)) {
        // Recomputed ancestors keep the provenance emitted by this build. In particular, a continue-on-error fallback
        // must not be overwritten with the old trusted fingerprint.
        if (columnMetadata.getTransformFunctionProvenanceVersion() == ColumnMetadata.UNAVAILABLE) {
          properties.clearProperty(V1Constants.MetadataKeys.Column.getKeyFor(column,
              V1Constants.MetadataKeys.Column.TRANSFORM_FUNCTION));
          properties.clearProperty(V1Constants.MetadataKeys.Column.getKeyFor(column,
              V1Constants.MetadataKeys.Column.TRANSFORM_FUNCTION_BASE64));
          properties.clearProperty(V1Constants.MetadataKeys.Column.getKeyFor(column,
              V1Constants.MetadataKeys.Column.TRANSFORM_FUNCTION_PROVENANCE_VERSION));
          properties.clearProperty(V1Constants.MetadataKeys.Column.getKeyFor(column,
              V1Constants.MetadataKeys.Column.TRANSFORM_FUNCTION_FINGERPRINT));
        } else {
          BaseSegmentCreator.addTransformFunction(properties, column, columnMetadata.getTransformFunction(),
              columnMetadata.getTransformFunctionFingerprint());
          // Preserve the exact provenance version. addTransformFunction() normally infers the version for new writes,
          // but an unrelated refresh must not silently upgrade or downgrade metadata it did not regenerate.
          properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column,
                  V1Constants.MetadataKeys.Column.TRANSFORM_FUNCTION_PROVENANCE_VERSION),
              columnMetadata.getTransformFunctionProvenanceVersion());
        }
      }
    }
    for (String column : replayDefaultColumns) {
      // Row replay supplies this value before expressions, but the regular segment-generation path otherwise records
      // it like a source field. Preserve its default-column identity so a later structural or transform change can
      // regenerate it instead of treating the previously materialized default as authoritative input.
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column,
          V1Constants.MetadataKeys.Column.IS_AUTO_GENERATED), true);
      BaseSegmentCreator.addTransformFunction(properties, column, null);
    }
    SegmentMetadataUtils.savePropertiesConfiguration(properties, refreshedSegmentDir);
  }

  private static void closeSegmentDirectoryQuietly(SegmentDirectory segmentDirectory) {
    if (segmentDirectory != null) {
      try {
        segmentDirectory.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close SegmentDirectory due to error: {}", e.getMessage());
      }
    }
  }

  /// Supplies current schema defaults before expression evaluation when replay intentionally omits a mutable default
  /// column. The ordinary null-value transformer runs after expressions, which is too late for dependent transforms.
  private static final class ReplayDefaultValueTransformer implements RecordTransformer {
    private final Map<String, Object> _defaultValues;

    private ReplayDefaultValueTransformer(Map<String, Object> defaultValues) {
      _defaultValues = defaultValues;
    }

    @Override
    public void transform(GenericRow record) {
      for (Map.Entry<String, Object> entry : _defaultValues.entrySet()) {
        record.putDefaultNullValue(entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
      SegmentConversionResult segmentConversionResult) {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE,
        Map.of(MinionConstants.RefreshSegmentTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
            MinionTaskUtils.toUTCString(_taskStartTime)));
  }
}
