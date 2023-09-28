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
package org.apache.pinot.segment.local.segment.index.loader.defaultcolumn;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.segment.local.function.FunctionEvaluator;
import org.apache.pinot.segment.local.function.FunctionEvaluatorFactory;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentColumnarIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueUnsortedForwardIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueSortedForwardIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueUnsortedForwardIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.nullvalue.NullValueVectorCreator;
import org.apache.pinot.segment.local.segment.creator.impl.stats.BytesColumnPredIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.DoubleColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.FloatColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.IntColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.LongColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.StringColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.VectorColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexType;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.ColumnIndexCreationInfo;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.data.FieldSpec.FieldType.DATE_TIME;
import static org.apache.pinot.spi.data.FieldSpec.FieldType.DIMENSION;
import static org.apache.pinot.spi.data.FieldSpec.FieldType.METRIC;


public abstract class BaseDefaultColumnHandler implements DefaultColumnHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseDefaultColumnHandler.class);

  protected enum DefaultColumnAction {
    // Present in schema but not in segment.
    ADD_DIMENSION,
    ADD_METRIC,
    ADD_DATE_TIME,
    // Present in segment but not in schema
    REMOVE_DIMENSION,
    REMOVE_METRIC,
    REMOVE_DATE_TIME,
    // Present in both segment and schema but one of the following updates is needed
    UPDATE_DIMENSION_DATA_TYPE,
    UPDATE_DIMENSION_DEFAULT_VALUE,
    UPDATE_DIMENSION_NUMBER_OF_VALUES,
    UPDATE_METRIC_DATA_TYPE,
    UPDATE_METRIC_DEFAULT_VALUE,
    UPDATE_METRIC_NUMBER_OF_VALUES,
    UPDATE_DATE_TIME_DATA_TYPE,
    UPDATE_DATE_TIME_DEFAULT_VALUE;

    boolean isAddAction() {
      return this == ADD_DIMENSION || this == ADD_METRIC || this == ADD_DATE_TIME;
    }

    boolean isUpdateAction() {
      return !(isAddAction() || isRemoveAction());
    }

    boolean isRemoveAction() {
      return this == REMOVE_DIMENSION || this == REMOVE_METRIC || this == REMOVE_DATE_TIME;
    }
  }

  protected final File _indexDir;
  protected final SegmentMetadata _segmentMetadata;
  protected final IndexLoadingConfig _indexLoadingConfig;
  protected final Schema _schema;
  protected final SegmentDirectory.Writer _segmentWriter;

  // NOTE: _segmentProperties shouldn't be used when checking whether default column need to be created because at that
  //       time _segmentMetadata might not be loaded from a local file
  private PropertiesConfiguration _segmentProperties;

  protected BaseDefaultColumnHandler(File indexDir, SegmentMetadata segmentMetadata,
      IndexLoadingConfig indexLoadingConfig, Schema schema, SegmentDirectory.Writer segmentWriter) {
    _indexDir = indexDir;
    _segmentMetadata = segmentMetadata;
    _indexLoadingConfig = indexLoadingConfig;
    _schema = schema;
    _segmentWriter = segmentWriter;
  }

  @Override
  public boolean needUpdateDefaultColumns() {
    Map<String, DefaultColumnAction> defaultColumnActionMap = computeDefaultColumnActionMap();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Need to update default columns with actionMap: {}", defaultColumnActionMap);
    }
    return !defaultColumnActionMap.isEmpty();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateDefaultColumns()
      throws Exception {
    // Compute the action needed for each column.
    Map<String, DefaultColumnAction> defaultColumnActionMap = computeDefaultColumnActionMap();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Update default columns with actionMap: {}", defaultColumnActionMap);
    }
    if (defaultColumnActionMap.isEmpty()) {
      return;
    }

    // Update each default column based on the default column action.
    _segmentProperties = SegmentMetadataUtils.getPropertiesConfiguration(_segmentMetadata);
    Iterator<Map.Entry<String, DefaultColumnAction>> entryIterator = defaultColumnActionMap.entrySet().iterator();
    while (entryIterator.hasNext()) {
      Map.Entry<String, DefaultColumnAction> entry = entryIterator.next();
      // This method updates the metadata properties, need to save it later. Remove the entry if the update failed.
      if (!updateDefaultColumn(entry.getKey(), entry.getValue())) {
        entryIterator.remove();
      }
    }

    // Update the segment metadata.
    List<String> dimensionColumns =
        LoaderUtils.getStringListFromSegmentProperties(V1Constants.MetadataKeys.Segment.DIMENSIONS, _segmentProperties);
    List<String> metricColumns =
        LoaderUtils.getStringListFromSegmentProperties(V1Constants.MetadataKeys.Segment.METRICS, _segmentProperties);
    List<String> dateTimeColumns =
        LoaderUtils.getStringListFromSegmentProperties(V1Constants.MetadataKeys.Segment.DATETIME_COLUMNS,
            _segmentProperties);
    for (Map.Entry<String, DefaultColumnAction> entry : defaultColumnActionMap.entrySet()) {
      String column = entry.getKey();
      DefaultColumnAction action = entry.getValue();
      switch (action) {
        case ADD_DIMENSION:
          dimensionColumns.add(column);
          break;
        case ADD_METRIC:
          metricColumns.add(column);
          break;
        case ADD_DATE_TIME:
          dateTimeColumns.add(column);
          break;
        case REMOVE_DIMENSION:
          dimensionColumns.remove(column);
          break;
        case REMOVE_METRIC:
          metricColumns.remove(column);
          break;
        case REMOVE_DATE_TIME:
          dateTimeColumns.remove(column);
          break;
        default:
          break;
      }
    }
    _segmentProperties.setProperty(V1Constants.MetadataKeys.Segment.DIMENSIONS, dimensionColumns);
    _segmentProperties.setProperty(V1Constants.MetadataKeys.Segment.METRICS, metricColumns);
    _segmentProperties.setProperty(V1Constants.MetadataKeys.Segment.DATETIME_COLUMNS, dateTimeColumns);

    // Save the new metadata
    SegmentMetadataUtils.savePropertiesConfiguration(_segmentProperties);
  }

  /**
   * Compute the action needed for each column.
   * This method compares the column metadata across schema and segment.
   *
   * @return Action Map for each column.
   */
  @VisibleForTesting
  Map<String, DefaultColumnAction> computeDefaultColumnActionMap() {
    Map<String, DefaultColumnAction> defaultColumnActionMap = new HashMap<>();

    // Compute ADD and UPDATE actions.
    for (FieldSpec fieldSpecInSchema : _schema.getAllFieldSpecs()) {
      if (fieldSpecInSchema.isVirtualColumn()) {
        continue;
      }
      String column = fieldSpecInSchema.getName();
      FieldSpec.FieldType fieldTypeInSchema = fieldSpecInSchema.getFieldType();
      ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);

      if (columnMetadata != null) {
        // Column exists in the segment, check if we need to update the value.

        // Only check for auto-generated column.
        if (!columnMetadata.isAutoGenerated()) {
          continue;
        }

        // Check the field type matches.
        FieldSpec fieldSpecInMetadata = columnMetadata.getFieldSpec();
        FieldSpec.FieldType fieldTypeInMetadata = fieldSpecInMetadata.getFieldType();
        if (fieldTypeInMetadata != fieldTypeInSchema) {
          String failureMessage = "Field type: " + fieldTypeInMetadata + " for auto-generated column: " + column
              + " does not match field type: " + fieldTypeInSchema
              + " in schema, throw exception to drop and re-download the segment.";
          throw new RuntimeException(failureMessage);
        }

        // Check the data type and default value matches.
        DataType dataTypeInMetadata = fieldSpecInMetadata.getDataType();
        DataType dataTypeInSchema = fieldSpecInSchema.getDataType();
        boolean isSingleValueInMetadata = fieldSpecInMetadata.isSingleValueField();
        boolean isSingleValueInSchema = fieldSpecInSchema.isSingleValueField();
        String defaultValueInMetadata = fieldSpecInMetadata.getDefaultNullValueString();
        String defaultValueInSchema = fieldSpecInSchema.getDefaultNullValueString();

        if (fieldTypeInMetadata == DIMENSION) {
          if (dataTypeInMetadata != dataTypeInSchema) {
            defaultColumnActionMap.put(column, DefaultColumnAction.UPDATE_DIMENSION_DATA_TYPE);
          } else if (!defaultValueInSchema.equals(defaultValueInMetadata)) {
            defaultColumnActionMap.put(column, DefaultColumnAction.UPDATE_DIMENSION_DEFAULT_VALUE);
          } else if (isSingleValueInMetadata != isSingleValueInSchema) {
            defaultColumnActionMap.put(column, DefaultColumnAction.UPDATE_DIMENSION_NUMBER_OF_VALUES);
          }
        } else if (fieldTypeInMetadata == METRIC) {
          if (dataTypeInMetadata != dataTypeInSchema) {
            defaultColumnActionMap.put(column, DefaultColumnAction.UPDATE_METRIC_DATA_TYPE);
          } else if (!defaultValueInSchema.equals(defaultValueInMetadata)) {
            defaultColumnActionMap.put(column, DefaultColumnAction.UPDATE_METRIC_DEFAULT_VALUE);
          } else if (isSingleValueInMetadata != isSingleValueInSchema) {
            defaultColumnActionMap.put(column, DefaultColumnAction.UPDATE_METRIC_NUMBER_OF_VALUES);
          }
        } else if (fieldTypeInMetadata == DATE_TIME) {
          if (dataTypeInMetadata != dataTypeInSchema) {
            defaultColumnActionMap.put(column, DefaultColumnAction.UPDATE_DATE_TIME_DATA_TYPE);
          } else if (!defaultValueInSchema.equals(defaultValueInMetadata)) {
            defaultColumnActionMap.put(column, DefaultColumnAction.UPDATE_DATE_TIME_DEFAULT_VALUE);
          }
        }
      } else {
        // Column does not exist in the segment, add default value for it.

        switch (fieldTypeInSchema) {
          case DIMENSION:
            defaultColumnActionMap.put(column, DefaultColumnAction.ADD_DIMENSION);
            break;
          case METRIC:
            defaultColumnActionMap.put(column, DefaultColumnAction.ADD_METRIC);
            break;
          case DATE_TIME:
            defaultColumnActionMap.put(column, DefaultColumnAction.ADD_DATE_TIME);
            break;
          default:
            LOGGER.warn("Skip adding default column for column: {} with field type: {}", column, fieldTypeInSchema);
            break;
        }
      }
    }

    // Compute REMOVE actions.
    for (ColumnMetadata columnMetadata : _segmentMetadata.getColumnMetadataMap().values()) {
      String column = columnMetadata.getColumnName();
      // Only remove auto-generated columns
      if (!_schema.hasColumn(column) && columnMetadata.isAutoGenerated()) {
        FieldSpec.FieldType fieldTypeInMetadata = columnMetadata.getFieldSpec().getFieldType();
        if (fieldTypeInMetadata == DIMENSION) {
          defaultColumnActionMap.put(column, DefaultColumnAction.REMOVE_DIMENSION);
        } else if (fieldTypeInMetadata == METRIC) {
          defaultColumnActionMap.put(column, DefaultColumnAction.REMOVE_METRIC);
        } else if (fieldTypeInMetadata == DATE_TIME) {
          defaultColumnActionMap.put(column, DefaultColumnAction.REMOVE_DATE_TIME);
        }
      }
    }

    return defaultColumnActionMap;
  }

  /**
   * Helper method to update default column indices, returns {@code true} if the update succeeds, {@code false}
   * otherwise.
   */
  protected abstract boolean updateDefaultColumn(String column, DefaultColumnAction action)
      throws Exception;

  /**
   * Helper method to remove the indices (dictionary and forward index) for a default column.
   *
   * @param column column name.
   */
  protected void removeColumnIndices(String column)
      throws IOException {
    String segmentName = _segmentMetadata.getName();
    LOGGER.info("Removing default column: {} from segment: {}", column, segmentName);
    // Delete existing dictionary and forward index
    _segmentWriter.removeIndex(column, StandardIndexes.dictionary());
    _segmentWriter.removeIndex(column, StandardIndexes.forward());
    // Remove the column metadata
    SegmentColumnarIndexCreator.removeColumnMetadataInfo(_segmentProperties, column);
    LOGGER.info("Removed default column: {} from segment: {}", column, segmentName);
  }

  /**
   * Helper method to create the V1 indices (dictionary and forward index) for a column, returns {@code true} if the
   * creation succeeds, {@code false} otherwise.
   */
  protected boolean createColumnV1Indices(String column)
      throws Exception {
    TableConfig tableConfig = _indexLoadingConfig.getTableConfig();
    boolean errorOnFailure = _indexLoadingConfig.isErrorOnColumnBuildFailure();
    if (tableConfig != null && tableConfig.getIngestionConfig() != null
        && tableConfig.getIngestionConfig().getTransformConfigs() != null) {
      List<TransformConfig> transformConfigs = tableConfig.getIngestionConfig().getTransformConfigs();
      for (TransformConfig transformConfig : transformConfigs) {
        if (transformConfig.getColumnName().equals(column)) {
          String transformFunction = transformConfig.getTransformFunction();
          FunctionEvaluator functionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(transformFunction);

          // Check if all arguments exist in the segment
          // TODO: Support chained derived column
          List<String> arguments = functionEvaluator.getArguments();
          List<ColumnMetadata> argumentsMetadata = new ArrayList<>(arguments.size());
          for (String argument : arguments) {
            ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(argument);
            if (columnMetadata == null) {
              LOGGER.warn("Skip creating derived column: {} because argument: {} does not exist in the segment", column,
                  argument);
              if (errorOnFailure) {
                throw new RuntimeException(String.format(
                    "Failed to create derived column: %s because argument: %s does not exist in the segment", column,
                    argument));
              }
              return false;
            }
            // TODO: Support creation of derived columns from forward index disabled columns
            if (!_segmentWriter.hasIndexFor(argument, StandardIndexes.forward())) {
              throw new UnsupportedOperationException(String.format("Operation not supported! Cannot create a derived "
                  + "column %s because argument: %s does not have a forward index. Enable forward index and "
                  + "refresh/backfill the segments to create a derived column from source column %s", column, argument,
                  argument));
            }
            argumentsMetadata.add(columnMetadata);
          }

          // TODO: Support raw derived column
          if (_indexLoadingConfig.getNoDictionaryColumns().contains(column)) {
            LOGGER.warn("Skip creating raw derived column: {}", column);
            if (errorOnFailure) {
              throw new UnsupportedOperationException(String.format("Failed to create raw derived column: %s", column));
            }
            return false;
          }

          // TODO: Support forward index disabled derived column
          if (_indexLoadingConfig.getForwardIndexDisabledColumns().contains(column)) {
            LOGGER.warn("Skip creating forward index disabled derived column: {}", column);
            if (errorOnFailure) {
              throw new UnsupportedOperationException(
                  String.format("Failed to create forward index disabled derived column: %s", column));
            }
            return false;
          }

          try {
            createDerivedColumnV1Indices(column, functionEvaluator, argumentsMetadata);
            return true;
          } catch (Exception e) {
            LOGGER.error("Caught exception while creating derived column: {} with transform function: {}", column,
                transformFunction, e);
            if (errorOnFailure) {
              throw e;
            }
            return false;
          }
        }
      }
    }

    createDefaultValueColumnV1Indices(column);
    return true;
  }

  /**
   * Check and return whether the forward index is disabled for a given column
   */
  protected boolean isForwardIndexDisabled(String column) {
    return _indexLoadingConfig.getForwardIndexDisabledColumns() != null
        && _indexLoadingConfig.getForwardIndexDisabledColumns().contains(column);
  }

  /**
   * Helper method to create the V1 indices (dictionary and forward index) for a column with default values.
   */
  private void createDefaultValueColumnV1Indices(String column)
      throws Exception {
    FieldSpec fieldSpec = _schema.getFieldSpecFor(column);

    // Generate column index creation information.
    int totalDocs = _segmentMetadata.getTotalDocs();
    DataType dataType = fieldSpec.getDataType();
    Object defaultValue = fieldSpec.getDefaultNullValue();
    boolean isSingleValue = fieldSpec.isSingleValueField();
    int maxNumberOfMultiValueElements = isSingleValue ? 0 : 1;

    Object sortedArray;
    switch (dataType.getStoredType()) {
      case INT:
        Preconditions.checkState(defaultValue instanceof Integer);
        sortedArray = new int[]{(Integer) defaultValue};
        break;
      case LONG:
        Preconditions.checkState(defaultValue instanceof Long);
        sortedArray = new long[]{(Long) defaultValue};
        break;
      case FLOAT:
        Preconditions.checkState(defaultValue instanceof Float);
        sortedArray = new float[]{(Float) defaultValue};
        break;
      case DOUBLE:
        Preconditions.checkState(defaultValue instanceof Double);
        sortedArray = new double[]{(Double) defaultValue};
        break;
      case BIG_DECIMAL:
        Preconditions.checkState(defaultValue instanceof BigDecimal);
        sortedArray = new BigDecimal[]{(BigDecimal) defaultValue};
        break;
      case STRING:
        Preconditions.checkState(defaultValue instanceof String);
        sortedArray = new String[]{(String) defaultValue};
        break;
      case BYTES:
        Preconditions.checkState(defaultValue instanceof byte[]);
        // Convert byte[] to ByteArray for internal usage
        ByteArray bytesDefaultValue = new ByteArray((byte[]) defaultValue);
        defaultValue = bytesDefaultValue;
        sortedArray = new ByteArray[]{bytesDefaultValue};
        break;
      case VECTOR:
        Preconditions.checkState(defaultValue instanceof Vector);
        sortedArray = new Vector[]{(Vector) defaultValue};
        break;
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType + " for column: " + column);
    }
    DefaultColumnStatistics columnStatistics =
        new DefaultColumnStatistics(defaultValue  /* min */, defaultValue  /* max */, sortedArray, isSingleValue,
            totalDocs, maxNumberOfMultiValueElements);

    ColumnIndexCreationInfo columnIndexCreationInfo =
        new ColumnIndexCreationInfo(columnStatistics, true/*createDictionary*/, false, true/*isAutoGenerated*/,
            defaultValue/*defaultNullValue*/);

    // Create dictionary.
    // We will have only one value in the dictionary.
    int dictionaryElementSize;
    try (SegmentDictionaryCreator creator = new SegmentDictionaryCreator(fieldSpec, _indexDir, false)) {
      creator.build(sortedArray);
      dictionaryElementSize = creator.getNumBytesPerEntry();
    }

    // Create forward index.
    if (isSingleValue) {
      // Single-value column.

      try (SingleValueSortedForwardIndexCreator svFwdIndexCreator = new SingleValueSortedForwardIndexCreator(_indexDir,
          fieldSpec.getName(), 1/*cardinality*/)) {
        for (int docId = 0; docId < totalDocs; docId++) {
          svFwdIndexCreator.putDictId(0);
        }
      }
    } else {
      // Multi-value column.

      boolean forwardIndexDisabled = isForwardIndexDisabled(column);
      if (forwardIndexDisabled) {
        // Generate an inverted index instead of forward index for multi-value columns when forward index is disabled
        try (DictionaryBasedInvertedIndexCreator creator = new OffHeapBitmapInvertedIndexCreator(_indexDir, fieldSpec,
            1, totalDocs, totalDocs)) {
          int[] dictIds = new int[]{0};
          for (int docId = 0; docId < totalDocs; docId++) {
            creator.add(dictIds, 1);
          }
          creator.seal();
        }
      } else {
        try (MultiValueUnsortedForwardIndexCreator mvFwdIndexCreator = new MultiValueUnsortedForwardIndexCreator(
            _indexDir, fieldSpec.getName(), 1/*cardinality*/, totalDocs/*numDocs*/,
            totalDocs/*totalNumberOfValues*/)) {
          int[] dictIds = {0};
          for (int docId = 0; docId < totalDocs; docId++) {
            mvFwdIndexCreator.putDictIdMV(dictIds);
          }
        }
      }
    }

    if (_indexLoadingConfig.getTableConfig() != null
        && _indexLoadingConfig.getTableConfig().getIndexingConfig() != null
        && _indexLoadingConfig.getTableConfig().getIndexingConfig().isNullHandlingEnabled()) {
      if (!_segmentWriter.hasIndexFor(column, StandardIndexes.nullValueVector())) {
        try (NullValueVectorCreator nullValueVectorCreator =
            new NullValueVectorCreator(_indexDir, fieldSpec.getName())) {
          for (int docId = 0; docId < totalDocs; docId++) {
            nullValueVectorCreator.setNull(docId);
          }

          nullValueVectorCreator.seal();
        }
      }
    }

    // Add the column metadata information to the metadata properties.
    SegmentColumnarIndexCreator.addColumnMetadataInfo(_segmentProperties, column, columnIndexCreationInfo, totalDocs,
        fieldSpec, true/*hasDictionary*/, dictionaryElementSize);
  }

  /**
   * Helper method to create the V1 indices (dictionary and forward index) for a column with derived values.
   * TODO:
   *   - Support chained derived column
   *   - Support raw derived column
   *   - Support forward index disabled derived column
   */
  private void createDerivedColumnV1Indices(String column, FunctionEvaluator functionEvaluator,
      List<ColumnMetadata> argumentsMetadata)
      throws Exception {
    // Initialize value readers for all arguments
    int numArguments = argumentsMetadata.size();
    List<ValueReader> valueReaders = new ArrayList<>(numArguments);
    for (ColumnMetadata argumentMetadata : argumentsMetadata) {
      valueReaders.add(new ValueReader(argumentMetadata));
    }

    try {
      // Calculate the values for the derived column
      Object[] inputValues = new Object[numArguments];
      int numDocs = _segmentMetadata.getTotalDocs();
      Object[] outputValues = new Object[numDocs];
      PinotDataType outputValueType = null;
      for (int i = 0; i < numDocs; i++) {
        for (int j = 0; j < numArguments; j++) {
          inputValues[j] = valueReaders.get(j).getValue(i);
        }
        Object outputValue = functionEvaluator.evaluate(inputValues);
        outputValues[i] = outputValue;
        if (outputValueType == null) {
          Class<?> outputValueClass = outputValue.getClass();
          outputValueType = FunctionUtils.getParameterType(outputValueClass);
          Preconditions.checkState(outputValueType != null, "Unsupported output value class: %s", outputValueClass);
        }
      }

      FieldSpec fieldSpec = _schema.getFieldSpecFor(column);
      StatsCollectorConfig statsCollectorConfig =
          new StatsCollectorConfig(_indexLoadingConfig.getTableConfig(), _schema, null);
      ColumnIndexCreationInfo indexCreationInfo;
      boolean isSingleValue = fieldSpec.isSingleValueField();
      switch (fieldSpec.getDataType().getStoredType()) {
        case INT: {
          for (int i = 0; i < numDocs; i++) {
            Object outputValue = outputValues[i];
            if (isSingleValue) {
              outputValues[i] = outputValueType.toInt(outputValue);
            } else {
              Integer[] values = outputValueType.toIntegerArray(outputValue);
              if (values.length == 0) {
                values = new Integer[]{(Integer) fieldSpec.getDefaultNullValue()};
              }
              outputValues[i] = values;
            }
          }
          IntColumnPreIndexStatsCollector statsCollector =
              new IntColumnPreIndexStatsCollector(column, statsCollectorConfig);
          for (Object value : outputValues) {
            statsCollector.collect(value);
          }
          statsCollector.seal();
          indexCreationInfo =
              new ColumnIndexCreationInfo(statsCollector, true, false, true, fieldSpec.getDefaultNullValue());
          break;
        }
        case LONG: {
          for (int i = 0; i < numDocs; i++) {
            Object outputValue = outputValues[i];
            if (isSingleValue) {
              outputValues[i] = outputValueType.toLong(outputValue);
            } else {
              Long[] values = outputValueType.toLongArray(outputValue);
              if (values.length == 0) {
                values = new Long[]{(Long) fieldSpec.getDefaultNullValue()};
              }
              outputValues[i] = values;
            }
          }
          LongColumnPreIndexStatsCollector statsCollector =
              new LongColumnPreIndexStatsCollector(column, statsCollectorConfig);
          for (Object value : outputValues) {
            statsCollector.collect(value);
          }
          statsCollector.seal();
          indexCreationInfo =
              new ColumnIndexCreationInfo(statsCollector, true, false, true, fieldSpec.getDefaultNullValue());
          break;
        }
        case FLOAT: {
          for (int i = 0; i < numDocs; i++) {
            Object outputValue = outputValues[i];
            if (isSingleValue) {
              outputValues[i] = outputValueType.toFloat(outputValue);
            } else {
              Float[] values = outputValueType.toFloatArray(outputValue);
              if (values.length == 0) {
                values = new Float[]{(Float) fieldSpec.getDefaultNullValue()};
              }
              outputValues[i] = values;
            }
          }
          FloatColumnPreIndexStatsCollector statsCollector =
              new FloatColumnPreIndexStatsCollector(column, statsCollectorConfig);
          for (Object value : outputValues) {
            statsCollector.collect(value);
          }
          statsCollector.seal();
          indexCreationInfo =
              new ColumnIndexCreationInfo(statsCollector, true, false, true, fieldSpec.getDefaultNullValue());
          break;
        }
        case DOUBLE: {
          for (int i = 0; i < numDocs; i++) {
            Object outputValue = outputValues[i];
            if (isSingleValue) {
              outputValues[i] = outputValueType.toDouble(outputValue);
            } else {
              Double[] values = outputValueType.toDoubleArray(outputValue);
              if (values.length == 0) {
                values = new Double[]{(Double) fieldSpec.getDefaultNullValue()};
              }
              outputValues[i] = values;
            }
          }
          DoubleColumnPreIndexStatsCollector statsCollector =
              new DoubleColumnPreIndexStatsCollector(column, statsCollectorConfig);
          for (Object value : outputValues) {
            statsCollector.collect(value);
          }
          statsCollector.seal();
          indexCreationInfo =
              new ColumnIndexCreationInfo(statsCollector, true, false, true, fieldSpec.getDefaultNullValue());
          break;
        }
        case BIG_DECIMAL: {
          for (int i = 0; i < numDocs; i++) {
            Preconditions.checkState(isSingleValue, "MV BIG_DECIMAL is not supported");
            outputValues[i] = outputValueType.toBigDecimal(outputValues[i]);
          }
          DoubleColumnPreIndexStatsCollector statsCollector =
              new DoubleColumnPreIndexStatsCollector(column, statsCollectorConfig);
          for (Object value : outputValues) {
            statsCollector.collect(value);
          }
          statsCollector.seal();
          indexCreationInfo =
              new ColumnIndexCreationInfo(statsCollector, true, false, true, fieldSpec.getDefaultNullValue());
          break;
        }
        case STRING: {
          for (int i = 0; i < numDocs; i++) {
            Object outputValue = outputValues[i];
            if (isSingleValue) {
              outputValues[i] = outputValueType.toString(outputValue);
            } else {
              String[] values = outputValueType.toStringArray(outputValue);
              if (values.length == 0) {
                values = new String[]{(String) fieldSpec.getDefaultNullValue()};
              }
              outputValues[i] = values;
            }
          }
          StringColumnPreIndexStatsCollector statsCollector =
              new StringColumnPreIndexStatsCollector(column, statsCollectorConfig);
          for (Object value : outputValues) {
            statsCollector.collect(value);
          }
          statsCollector.seal();
          indexCreationInfo = new ColumnIndexCreationInfo(statsCollector, true,
              _indexLoadingConfig.getVarLengthDictionaryColumns().contains(column), true,
              fieldSpec.getDefaultNullValue());
          break;
        }
        case BYTES: {
          for (int i = 0; i < numDocs; i++) {
            Object outputValue = outputValues[i];
            if (isSingleValue) {
              outputValues[i] = outputValueType.toBytes(outputValue);
            } else {
              byte[][] values = outputValueType.toBytesArray(outputValue);
              if (values.length == 0) {
                values = new byte[][]{(byte[]) fieldSpec.getDefaultNullValue()};
              }
              outputValues[i] = values;
            }
          }
          BytesColumnPredIndexStatsCollector statsCollector =
              new BytesColumnPredIndexStatsCollector(column, statsCollectorConfig);
          for (Object value : outputValues) {
            statsCollector.collect(value);
          }
          statsCollector.seal();
          boolean useVarLengthDictionary;
          if (!statsCollector.isFixedLength()) {
            useVarLengthDictionary = true;
          } else {
            useVarLengthDictionary = _indexLoadingConfig.getVarLengthDictionaryColumns().contains(column);
          }
          indexCreationInfo = new ColumnIndexCreationInfo(statsCollector, true, useVarLengthDictionary, true,
              new ByteArray((byte[]) fieldSpec.getDefaultNullValue()));
          break;
        }
        case VECTOR: {
          for (int i = 0; i < numDocs; i++) {
            Preconditions.checkState(isSingleValue, "MV VECTOR is not supported");
            outputValues[i] = outputValueType.toVector(outputValues[i]);
          }
          VectorColumnPreIndexStatsCollector statsCollector =
              new VectorColumnPreIndexStatsCollector(column, statsCollectorConfig);
          for (Object value : outputValues) {
            statsCollector.collect(value);
          }
          statsCollector.seal();
          indexCreationInfo =
              new ColumnIndexCreationInfo(statsCollector, true, false, true, fieldSpec.getDefaultNullValue());
          break;
        }
        default:
          throw new IllegalStateException();
      }

      // Create dictionary
      try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(fieldSpec, _indexDir,
          indexCreationInfo.isUseVarLengthDictionary())) {
        dictionaryCreator.build(indexCreationInfo.getSortedUniqueElementsArray());

        // Create forward index
        int cardinality = indexCreationInfo.getDistinctValueCount();
        if (isSingleValue) {
          try (ForwardIndexCreator forwardIndexCreator = indexCreationInfo.isSorted()
              ? new SingleValueSortedForwardIndexCreator(_indexDir, column, cardinality)
              : new SingleValueUnsortedForwardIndexCreator(_indexDir, column, cardinality, numDocs)) {
            for (int i = 0; i < numDocs; i++) {
              forwardIndexCreator.putDictId(dictionaryCreator.indexOfSV(outputValues[i]));
            }
          }
        } else {
          try (ForwardIndexCreator forwardIndexCreator = new MultiValueUnsortedForwardIndexCreator(_indexDir, column,
              cardinality, numDocs, indexCreationInfo.getTotalNumberOfEntries())) {
            for (int i = 0; i < numDocs; i++) {
              forwardIndexCreator.putDictIdMV(dictionaryCreator.indexOfMV(outputValues[i]));
            }
          }
        }

        // Add the column metadata
        SegmentColumnarIndexCreator.addColumnMetadataInfo(_segmentProperties, column, indexCreationInfo, numDocs,
            fieldSpec, true, dictionaryCreator.getNumBytesPerEntry());
      }
    } finally {
      for (ValueReader valueReader : valueReaders) {
        valueReader.close();
      }
    }
  }

  @SuppressWarnings("rawtypes")
  private class ValueReader implements Closeable {
    final ForwardIndexReader _forwardIndexReader;
    final Dictionary _dictionary;
    final PinotSegmentColumnReader _columnReader;

    ValueReader(ColumnMetadata columnMetadata)
        throws IOException {
      _forwardIndexReader = ForwardIndexType.read(_segmentWriter, columnMetadata);
      if (columnMetadata.hasDictionary()) {
        _dictionary = DictionaryIndexType.read(_segmentWriter, columnMetadata);
      } else {
        _dictionary = null;
      }
      _columnReader = new PinotSegmentColumnReader(_forwardIndexReader, _dictionary, null,
          columnMetadata.getMaxNumberOfMultiValues());
    }

    Object getValue(int docId) {
      return _columnReader.getValue(docId);
    }

    @Override
    public void close()
        throws IOException {
      _columnReader.close();
      if (_dictionary != null) {
        _dictionary.close();
      }
      _forwardIndexReader.close();
    }
  }
}
