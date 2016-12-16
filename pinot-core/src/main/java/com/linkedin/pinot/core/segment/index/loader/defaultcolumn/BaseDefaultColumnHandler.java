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

  protected final File indexDir;
  protected final Schema schema;
  protected final SegmentMetadataImpl segmentMetadata;
  private final PropertiesConfiguration segmentProperties;

  protected BaseDefaultColumnHandler(File indexDir, Schema schema, SegmentMetadataImpl segmentMetadata) {
    this.indexDir = indexDir;
    this.schema = schema;
    this.segmentMetadata = segmentMetadata;
    segmentProperties = segmentMetadata.getSegmentMetadataPropertiesConfiguration();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateDefaultColumns()
      throws Exception {
    if (schema == null) {
      return;
    }

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
        LoaderUtils.getStringListFromSegmentProperties(V1Constants.MetadataKeys.Segment.DIMENSIONS, segmentProperties);
    List<String> metricColumns =
        LoaderUtils.getStringListFromSegmentProperties(V1Constants.MetadataKeys.Segment.METRICS, segmentProperties);
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
    segmentProperties.setProperty(V1Constants.MetadataKeys.Segment.DIMENSIONS, dimensionColumns);
    segmentProperties.setProperty(V1Constants.MetadataKeys.Segment.METRICS, metricColumns);

    // Create a back up for origin metadata.
    File metadataFile = new File(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    File metadataBackUpFile = new File(metadataFile + ".bak");
    if (!metadataBackUpFile.exists()) {
      FileUtils.copyFile(metadataFile, metadataBackUpFile);
    }

    // Save the new metadata.
    segmentProperties.save(metadataFile);
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
    Collection<String> columnsInSchema = schema.getColumnNames();
    for (String column : columnsInSchema) {
      FieldSpec fieldSpec = schema.getFieldSpecFor(column);
      FieldSpec.FieldType fieldType = fieldSpec.getFieldType();
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);

      if (columnMetadata != null) {
        // Column exists in the segment, check if we need to update the value.

        // Only check for auto-generated column.
        if (columnMetadata.isAutoGenerated()) {

          // Check the field type matches.
          FieldSpec.FieldType fieldTypeFromMetadata = columnMetadata.getFieldType();
          if (fieldTypeFromMetadata != fieldType) {
            String failureMessage = "Field type: " + fieldTypeFromMetadata + " for auto-generated column: " + column
                + " does not match field type: " + fieldType
                + " in schema, throw exception to drop and re-download the segment.";
            LOGGER.error(failureMessage);
            throw new RuntimeException(failureMessage);
          }

          // Check the data type and default value matches.
          FieldSpec.DataType dataTypeFromMetadata = columnMetadata.getDataType();
          FieldSpec.DataType dataTypeFromSchema = fieldSpec.getDataType();
          boolean isSingleValueInMetadata = columnMetadata.isSingleValue();
          boolean isSingleValueInSchema = fieldSpec.isSingleValueField();
          String defaultValueFromMetadata = columnMetadata.getDefaultNullValueString();
          String defaultValueFromSchema = fieldSpec.getDefaultNullValue().toString();
          if (dataTypeFromMetadata != dataTypeFromSchema || isSingleValueInMetadata != isSingleValueInSchema
              || !defaultValueFromSchema.equals(defaultValueFromMetadata)) {
            switch (fieldType) {
              case DIMENSION:
                defaultColumnActionMap.put(column, DefaultColumnAction.UPDATE_DIMENSION);
                break;
              case METRIC:
                defaultColumnActionMap.put(column, DefaultColumnAction.UPDATE_METRIC);
                break;
              default:
                throw new UnsupportedOperationException(
                    "Unsupported updating default column for column: " + column + " with field type: " + fieldType);
            }
          }
        }
      } else {
        // Column does not exist in the segment, add default value for it.

        switch (fieldType) {
          case DIMENSION:
            defaultColumnActionMap.put(column, DefaultColumnAction.ADD_DIMENSION);
            break;
          case METRIC:
            defaultColumnActionMap.put(column, DefaultColumnAction.ADD_METRIC);
            break;
          default:
            throw new UnsupportedOperationException(
                "Unsupported adding default column for column: " + column + " with field type: " + fieldType);
        }
      }
    }

    // Compute REMOVE actions.
    Set<String> columnsInSegment = segmentMetadata.getAllColumns();
    for (String column : columnsInSegment) {
      if (!columnsInSchema.contains(column)) {
        ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);

        // Only remove auto-generated columns.
        if (columnMetadata.isAutoGenerated()) {
          FieldSpec.FieldType fieldType = columnMetadata.getFieldType();
          switch (fieldType) {
            case DIMENSION:
              defaultColumnActionMap.put(column, DefaultColumnAction.REMOVE_DIMENSION);
              break;
            case METRIC:
              defaultColumnActionMap.put(column, DefaultColumnAction.REMOVE_METRIC);
              break;
            default:
              throw new UnsupportedOperationException(
                  "Unsupported removing default column for column: " + column + " with field type: " + fieldType);
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
    FieldSpec fieldSpec = schema.getFieldSpecFor(column);
    boolean isSingleValue = fieldSpec.isSingleValueField();

    // Delete existing dictionary and forward index for the column.
    File dictionaryFile = new File(indexDir, column + V1Constants.Dict.FILE_EXTENTION);
    File forwardIndexFile;
    if (isSingleValue) {
      forwardIndexFile = new File(indexDir, column + V1Constants.Indexes.SORTED_FWD_IDX_FILE_EXTENTION);
    } else {
      forwardIndexFile = new File(indexDir, column + V1Constants.Indexes.UN_SORTED_MV_FWD_IDX_FILE_EXTENTION);
    }
    FileUtils.deleteQuietly(dictionaryFile);
    FileUtils.deleteQuietly(forwardIndexFile);

    // Remove the column metadata information if exists.
    SegmentColumnarIndexCreator.removeColumnMetadataInfo(segmentProperties, column);
  }

  /**
   * Helper method to create the V1 indices (dictionary and forward index) for a column.
   *
   * @param column column name.
   */
  protected void createColumnV1Indices(String column)
      throws Exception {
    FieldSpec fieldSpec = schema.getFieldSpecFor(column);
    boolean isSingleValue = fieldSpec.isSingleValueField();

    // Generate column index creation information.
    int totalDocs = segmentMetadata.getTotalDocs();
    int totalRawDocs = segmentMetadata.getTotalRawDocs();
    int totalAggDocs = totalDocs - totalRawDocs;
    Object defaultValue = fieldSpec.getDefaultNullValue();
    String stringDefaultValue = defaultValue.toString();
    FieldSpec.DataType dataType = fieldSpec.getDataType();
    int maxNumberOfMultiValueElements = isSingleValue ? 0 : 1;
    int dictionaryElementSize = 0;
    Object sortedArray;

    switch (dataType) {
      case BOOLEAN:
      case STRING:
        // Length of the UTF-8 encoded byte array.
        dictionaryElementSize = stringDefaultValue.getBytes("UTF8").length;
        sortedArray = new String[]{stringDefaultValue};
        break;
      case INT:
        sortedArray = new int[]{Integer.valueOf(stringDefaultValue)};
        break;
      case LONG:
        sortedArray = new long[]{Long.valueOf(stringDefaultValue)};
        break;
      case FLOAT:
        sortedArray = new float[]{Float.valueOf(stringDefaultValue)};
        break;
      case DOUBLE:
        sortedArray = new double[]{Double.valueOf(stringDefaultValue)};
        break;
      default:
        throw new UnsupportedOperationException(
            "Schema evolution not supported for data type:" + fieldSpec.getDataType());
    }
    ColumnIndexCreationInfo columnIndexCreationInfo =
        new ColumnIndexCreationInfo(true/*createDictionary*/, defaultValue/*min*/, defaultValue/*max*/, sortedArray,
            ForwardIndexType.FIXED_BIT_COMPRESSED, InvertedIndexType.SORTED_INDEX, isSingleValue/*isSortedColumn*/,
            false/*hasNulls*/, totalDocs/*totalNumberOfEntries*/, maxNumberOfMultiValueElements, -1 /* Unused max length*/,
            true/*isAutoGenerated*/, defaultValue/*defaultNullValue*/);

    // Create dictionary.
    // We will have only one value in the dictionary.
    SegmentDictionaryCreator segmentDictionaryCreator =
        new SegmentDictionaryCreator(false/*hasNulls*/, sortedArray, fieldSpec, indexDir,
            V1Constants.Str.DEFAULT_STRING_PAD_CHAR);
    segmentDictionaryCreator.build(new boolean[]{true}/*isSorted*/);
    segmentDictionaryCreator.close();

    // Create forward index.
    if (isSingleValue) {
      // Single-value column.

      SingleValueSortedForwardIndexCreator svFwdIndexCreator =
          new SingleValueSortedForwardIndexCreator(indexDir, 1/*cardinality*/, fieldSpec);
      for (int docId = 0; docId < totalDocs; docId++) {
        svFwdIndexCreator.add(0/*dictionaryId*/, docId);
      }
      svFwdIndexCreator.close();
    } else {
      // Multi-value column.

      MultiValueUnsortedForwardIndexCreator mvFwdIndexCreator =
          new MultiValueUnsortedForwardIndexCreator(fieldSpec, indexDir, 1/*cardinality*/, totalDocs/*numDocs*/,
              totalDocs/*totalNumberOfValues*/, false/*hasNulls*/);
      int[] dictionaryIds = {0};
      for (int docId = 0; docId < totalDocs; docId++) {
        mvFwdIndexCreator.index(docId, dictionaryIds);
      }
      mvFwdIndexCreator.close();
    }

    // Add the column metadata information to the metadata properties.
    SegmentColumnarIndexCreator.addColumnMetadataInfo(segmentProperties, column, columnIndexCreationInfo, totalDocs,
        totalRawDocs, totalAggDocs, fieldSpec, true/*hasDictionary*/, dictionaryElementSize, true/*hasInvertedIndex*/,
        null/*hllOriginColumn*/);
  }
}
