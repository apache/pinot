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
package com.linkedin.pinot.core.segment.index.loader.defaultcolumn;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.segment.creator.ColumnIndexCreationInfo;
import com.linkedin.pinot.core.segment.creator.ForwardIndexType;
import com.linkedin.pinot.core.segment.creator.InvertedIndexType;
import com.linkedin.pinot.core.segment.creator.impl.SegmentColumnarIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.SegmentDictionaryCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.creator.impl.fwd.MultiValueUnsortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.SingleValueSortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.LoaderUtils;
import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class BaseDefaultColumnHandler implements DefaultColumnHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseDefaultColumnHandler.class);

  protected enum DefaultColumnAction {
    // Present in schema but not in segment.
    ADD_DIMENSION,
    ADD_METRIC,
    // Present in schema & segment but default value doesn't match.
    UPDATE_DIMENSION,
    UPDATE_METRIC,
    // Present in segment but not in schema, auto-generated.
    REMOVE_DIMENSION,
    REMOVE_METRIC;

    boolean isRemoveAction() {
      return this == REMOVE_DIMENSION || this == REMOVE_METRIC;
    }
  }

  protected final File _indexDir;
  protected final Schema _schema;
  protected final SegmentMetadataImpl _segmentMetadata;

  private final PropertiesConfiguration _segmentProperties;

  protected BaseDefaultColumnHandler(File indexDir, Schema schema, SegmentMetadataImpl segmentMetadata) {
    _indexDir = indexDir;
    _schema = schema;
    _segmentMetadata = segmentMetadata;
    _segmentProperties = segmentMetadata.getSegmentMetadataPropertiesConfiguration();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateDefaultColumns()
      throws Exception {
    // Compute the action needed for each column.
    Map<String, DefaultColumnAction> defaultColumnActionMap = computeDefaultColumnActionMap();
    if (defaultColumnActionMap.isEmpty()) {
      return;
    }

    // Update each default column based on the default column action.
    for (Map.Entry<String, DefaultColumnAction> entry : defaultColumnActionMap.entrySet()) {
      // This method updates the metadata properties, need to save it later.
      updateDefaultColumn(entry.getKey(), entry.getValue());
    }

    // Update the segment metadata.
    List<String> dimensionColumns =
        LoaderUtils.getStringListFromSegmentProperties(V1Constants.MetadataKeys.Segment.DIMENSIONS, _segmentProperties);
    List<String> metricColumns =
        LoaderUtils.getStringListFromSegmentProperties(V1Constants.MetadataKeys.Segment.METRICS, _segmentProperties);
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
        case REMOVE_DIMENSION:
          dimensionColumns.remove(column);
          break;
        case REMOVE_METRIC:
          metricColumns.remove(column);
          break;
        default:
          break;
      }
    }
    _segmentProperties.setProperty(V1Constants.MetadataKeys.Segment.DIMENSIONS, dimensionColumns);
    _segmentProperties.setProperty(V1Constants.MetadataKeys.Segment.METRICS, metricColumns);

    // Create a back up for origin metadata.
    File metadataFile = _segmentProperties.getFile();
    File metadataBackUpFile = new File(metadataFile + ".bak");
    if (!metadataBackUpFile.exists()) {
      FileUtils.copyFile(metadataFile, metadataBackUpFile);
    }

    // Save the new metadata.
    _segmentProperties.save();
  }

  /**
   * Compute the action needed for each column.
   * This method compares the column metadata across schema and segment.
   *
   * @return Action Map for each column.
   */
  private Map<String, DefaultColumnAction> computeDefaultColumnActionMap() {
    Map<String, DefaultColumnAction> defaultColumnActionMap = new HashMap<>();

    // Compute ADD and UPDATE actions.
    Collection<String> columnsInSchema = _schema.getColumnNames();
    for (String column : columnsInSchema) {
      FieldSpec fieldSpecInSchema = _schema.getFieldSpecFor(column);
      Preconditions.checkNotNull(fieldSpecInSchema);
      FieldSpec.FieldType fieldTypeInSchema = fieldSpecInSchema.getFieldType();
      ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);

      if (columnMetadata != null) {
        // Column exists in the segment, check if we need to update the value.

        // Only check for auto-generated column.
        if (!columnMetadata.isAutoGenerated()) {
          continue;
        }

        // Check the field type matches.
        FieldSpec.FieldType fieldTypeInMetadata = columnMetadata.getFieldType();
        if (fieldTypeInMetadata != fieldTypeInSchema) {
          String failureMessage = "Field type: " + fieldTypeInMetadata + " for auto-generated column: " + column
              + " does not match field type: " + fieldTypeInSchema
              + " in schema, throw exception to drop and re-download the segment.";
          throw new RuntimeException(failureMessage);
        }

        // Check the data type and default value matches.
        FieldSpec.DataType dataTypeInMetadata = columnMetadata.getDataType();
        FieldSpec.DataType dataTypeInSchema = fieldSpecInSchema.getDataType();
        boolean isSingleValueInMetadata = columnMetadata.isSingleValue();
        boolean isSingleValueInSchema = fieldSpecInSchema.isSingleValueField();
        String defaultValueInMetadata = columnMetadata.getDefaultNullValueString();
        String defaultValueInSchema = fieldSpecInSchema.getDefaultNullValue().toString();
        if (dataTypeInMetadata != dataTypeInSchema || isSingleValueInMetadata != isSingleValueInSchema
            || !defaultValueInSchema.equals(defaultValueInMetadata)) {
          if (fieldTypeInMetadata == FieldSpec.FieldType.DIMENSION) {
            defaultColumnActionMap.put(column, DefaultColumnAction.UPDATE_DIMENSION);
          } else {
            Preconditions.checkState(fieldTypeInMetadata == FieldSpec.FieldType.METRIC);
            defaultColumnActionMap.put(column, DefaultColumnAction.UPDATE_METRIC);
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
          default:
            LOGGER.warn("Skip adding default column for column: {} with field type: {}", column, fieldTypeInSchema);
            break;
        }
      }
    }

    // Compute REMOVE actions.
    Set<String> columnsInMetadata = _segmentMetadata.getAllColumns();
    for (String column : columnsInMetadata) {
      if (!columnsInSchema.contains(column)) {
        ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);

        // Only remove auto-generated columns.
        if (columnMetadata.isAutoGenerated()) {
          FieldSpec.FieldType fieldTypeInMetadata = columnMetadata.getFieldType();
          if (fieldTypeInMetadata == FieldSpec.FieldType.DIMENSION) {
            defaultColumnActionMap.put(column, DefaultColumnAction.REMOVE_DIMENSION);
          } else {
            Preconditions.checkState(fieldTypeInMetadata == FieldSpec.FieldType.METRIC);
            defaultColumnActionMap.put(column, DefaultColumnAction.REMOVE_METRIC);
          }
        }
      }
    }

    return defaultColumnActionMap;
  }

  /**
   * Helper method to update default column indices.
   * TODO: ADD SUPPORT TO STAR TREE INDEX.
   *
   * @param column column name.
   * @param action default column action.
   * @throws Exception
   */
  protected abstract void updateDefaultColumn(String column, DefaultColumnAction action)
      throws Exception;

  /**
   * Helper method to remove the V1 indices (dictionary and forward index) for a column.
   *
   * @param column column name.
   */
  protected void removeColumnV1Indices(String column) {
    // Delete existing dictionary and forward index for the column.
    FileUtils.deleteQuietly(new File(_indexDir, column + V1Constants.Dict.FILE_EXTENSION));
    FileUtils.deleteQuietly(new File(_indexDir, column + V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION));
    FileUtils.deleteQuietly(new File(_indexDir, column + V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION));

    // Remove the column metadata information if exists.
    SegmentColumnarIndexCreator.removeColumnMetadataInfo(_segmentProperties, column);
  }

  /**
   * Helper method to create the V1 indices (dictionary and forward index) for a column.
   *
   * @param column column name.
   */
  protected void createColumnV1Indices(String column)
      throws Exception {
    FieldSpec fieldSpec = _schema.getFieldSpecFor(column);
    Preconditions.checkNotNull(fieldSpec);

    // Generate column index creation information.
    int totalDocs = _segmentMetadata.getTotalDocs();
    int totalRawDocs = _segmentMetadata.getTotalRawDocs();
    int totalAggDocs = totalDocs - totalRawDocs;
    FieldSpec.DataType dataType = fieldSpec.getDataType();
    Object defaultValue = fieldSpec.getDefaultNullValue();
    boolean isSingleValue = fieldSpec.isSingleValueField();
    int maxNumberOfMultiValueElements = isSingleValue ? 0 : 1;
    int dictionaryElementSize = 0;

    Object sortedArray;
    switch (dataType) {
      case STRING:
        Preconditions.checkState(defaultValue instanceof String);
        String stringDefaultValue = (String) defaultValue;
        // Length of the UTF-8 encoded byte array.
        dictionaryElementSize = stringDefaultValue.getBytes("UTF8").length;
        sortedArray = new String[]{stringDefaultValue};
        break;
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
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType + " for column: " + column);
    }

    ColumnIndexCreationInfo columnIndexCreationInfo =
        new ColumnIndexCreationInfo(true/*createDictionary*/, defaultValue/*min*/, defaultValue/*max*/, sortedArray,
            ForwardIndexType.FIXED_BIT_COMPRESSED, InvertedIndexType.SORTED_INDEX, isSingleValue/*isSortedColumn*/,
            false/*hasNulls*/, totalDocs/*totalNumberOfEntries*/, maxNumberOfMultiValueElements, -1 /* Unused max length*/,
            true/*isAutoGenerated*/, null/*partitionFunction*/, -1/*numPartitions*/, null/*partitionValue*/,
            defaultValue/*defaultNullValue*/);

    // Create dictionary.
    // We will have only one value in the dictionary.
    SegmentDictionaryCreator segmentDictionaryCreator =
        new SegmentDictionaryCreator(false/*hasNulls*/, sortedArray, fieldSpec, _indexDir,
            V1Constants.Str.DEFAULT_STRING_PAD_CHAR);
    segmentDictionaryCreator.build(new boolean[]{true}/*isSorted*/);
    segmentDictionaryCreator.close();

    // Create forward index.
    if (isSingleValue) {
      // Single-value column.

      SingleValueSortedForwardIndexCreator svFwdIndexCreator =
          new SingleValueSortedForwardIndexCreator(_indexDir, 1/*cardinality*/, fieldSpec);
      for (int docId = 0; docId < totalDocs; docId++) {
        svFwdIndexCreator.add(0/*dictionaryId*/, docId);
      }
      svFwdIndexCreator.close();
    } else {
      // Multi-value column.

      MultiValueUnsortedForwardIndexCreator mvFwdIndexCreator =
          new MultiValueUnsortedForwardIndexCreator(fieldSpec, _indexDir, 1/*cardinality*/, totalDocs/*numDocs*/,
              totalDocs/*totalNumberOfValues*/, false/*hasNulls*/);
      int[] dictionaryIds = {0};
      for (int docId = 0; docId < totalDocs; docId++) {
        mvFwdIndexCreator.index(docId, dictionaryIds);
      }
      mvFwdIndexCreator.close();
    }

    // Add the column metadata information to the metadata properties.
    SegmentColumnarIndexCreator.addColumnMetadataInfo(_segmentProperties, column, columnIndexCreationInfo, totalDocs,
        totalRawDocs, totalAggDocs, fieldSpec, true/*hasDictionary*/, dictionaryElementSize, true/*hasInvertedIndex*/,
        null/*hllOriginColumn*/);
  }
}
