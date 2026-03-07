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
package org.apache.pinot.plugin.inputformat.arrow;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for converting Apache Arrow VectorSchemaRoot to Pinot {@code GenericRow}. Processes
 * all fields and handles multiple rows from Arrow batch.
 */
public class ArrowToGenericRowConverter {
  private static final Logger logger = LoggerFactory.getLogger(ArrowToGenericRowConverter.class);

  /** Default constructor that processes all fields from Arrow batch. */
  public ArrowToGenericRowConverter() {
    logger.debug("ArrowToGenericRowConverter created for processing all fields");
  }

  /**
   * Converts an Arrow VectorSchemaRoot to a Pinot {@code GenericRow}. Processes ALL rows from the
   * Arrow batch and stores them as a list using MULTIPLE_RECORDS_KEY.
   *
   * @param reader ArrowStreamReader containing the data
   * @param root Arrow VectorSchemaRoot containing the data
   * @param destination Optional destination {@code GenericRow}, will create new if null
   * @return {@code GenericRow} containing {@code List<GenericRow>} with all converted rows, or null
   *     if no data available
   */
  @Nullable
  public GenericRow convert(
      ArrowStreamReader reader, VectorSchemaRoot root, GenericRow destination) {
    if (root == null) {
      logger.warn("Cannot convert null VectorSchemaRoot");
      return null;
    }

    if (destination == null) {
      destination = new GenericRow();
    }

    int rowCount = root.getRowCount();
    if (rowCount == 0) {
      logger.warn("No rows found in Arrow data");
      return destination;
    }

    List<GenericRow> rows = new ArrayList<>(rowCount);

    // Process all rows from the Arrow batch
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      GenericRow row = convertSingleRow(reader, root, rowIndex);
      if (row != null) {
        rows.add(row);
      }
    }

    if (!rows.isEmpty()) {
      // Use Pinot's MULTIPLE_RECORDS_KEY to store the list of rows
      destination.putValue(GenericRow.MULTIPLE_RECORDS_KEY, rows);
      logger.debug("Converted {} rows from Arrow batch", rows.size());
    }

    return destination;
  }

  /**
   * Converts a single row from Arrow VectorSchemaRoot.
   *
   * @param reader ArrowStreamReader containing the data
   * @param root Arrow VectorSchemaRoot containing the data
   * @param rowIndex Index of the row to convert (0-based)
   * @return {@code GenericRow} with converted data, or null if row index is invalid
   */
  @Nullable
  private GenericRow convertSingleRow(
      ArrowStreamReader reader, VectorSchemaRoot root, int rowIndex) {
    GenericRow row = new GenericRow();
    int convertedFields = 0;

    // Process all fields in the Arrow schema
    for (int i = 0; i < root.getFieldVectors().size(); i++) {
      FieldVector fieldVector = root.getFieldVectors().get(i);
      String fieldName = fieldVector.getField().getName();

      try {
        Object value;

        // Handle complex types with potential nested dictionary encoding
        if (fieldVector instanceof ListVector) {
          value = extractListValue((ListVector) fieldVector, rowIndex, reader);
        } else if (fieldVector instanceof MapVector) {
          value = extractMapValue((MapVector) fieldVector, rowIndex, reader);
        } else if (fieldVector instanceof StructVector) {
          value = extractStructValue((StructVector) fieldVector, rowIndex, reader);
        } else {
          // Handle simple types and top-level dictionary encoding
          value = extractValueFromVector(fieldVector, rowIndex, reader);
          // Convert Arrow-specific types to Pinot-compatible types
          value = convertArrowTypeToPinotCompatible(value);
        }

        if (value != null) {
          row.putValue(fieldName, value);
          convertedFields++;
        }
      } catch (Exception e) {
        logger.error("Error extracting value for field: {} at row {}", fieldName, rowIndex, e);
      }
    }

    logger.debug("Converted {} fields from Arrow row {} to GenericRow", convertedFields, rowIndex);
    return row;
  }

  /**
   * Extracts value from a FieldVector with dictionary decoding support. Handles null values and
   * recursively decodes nested dictionary-encoded structures.
   *
   * @param vector Source vector to extract from
   * @param index Row index to extract
   * @param reader ArrowStreamReader for dictionary lookup
   * @return Decoded value, or null if value is null
   */
  @Nullable
  private Object extractValueFromVector(FieldVector vector, int index, ArrowStreamReader reader) {
    // Null check first - preserve nulls throughout decoding
    if (vector.isNull(index)) {
      return null;
    }

    try {
      // Decode dictionary if present at this level
      if (vector.getField().getDictionary() != null) {
        long dictionaryId = vector.getField().getDictionary().getId();
        ValueVector dictionaryVector = reader.getDictionaryVectors().get(dictionaryId);
        if (dictionaryVector == null) {
          logger.error("Dictionary ID {} not found for field {}", dictionaryId, vector.getField().getName());
          return null;
        }
        try (ValueVector decodedVector = DictionaryEncoder.decode(vector, dictionaryVector)) {
          return decodedVector.getObject(index);
        }
      }

      // No dictionary encoding at this level
      return vector.getObject(index);
    } catch (Exception e) {
      logger.error("Error decoding value for field {} at index {}", vector.getField().getName(), index, e);
      return null;
    }
  }

  /**
   * Extracts and decodes a List value, handling dictionary-encoded elements.
   */
  @Nullable
  private List<Object> extractListValue(ListVector listVector, int index, ArrowStreamReader reader) {
    if (listVector.isNull(index)) {
      return null;
    }

    int startOffset = listVector.getOffsetBuffer().getInt(index * ListVector.OFFSET_WIDTH);
    int endOffset = listVector.getOffsetBuffer().getInt((index + 1) * ListVector.OFFSET_WIDTH);
    int elementCount = endOffset - startOffset;

    if (elementCount == 0) {
      return new ArrayList<>();
    }

    FieldVector dataVector = listVector.getDataVector();
    List<Object> result = new ArrayList<>(elementCount);

    for (int i = 0; i < elementCount; i++) {
      int elementIndex = startOffset + i;
      Object elementValue = extractValueFromVector(dataVector, elementIndex, reader);
      result.add(convertArrowTypeToPinotCompatible(elementValue));
    }

    return result;
  }

  /**
   * Extracts and decodes a Struct value, handling dictionary-encoded fields.
   */
  @Nullable
  private Map<String, Object> extractStructValue(StructVector structVector, int index,
      ArrowStreamReader reader) {
    if (structVector.isNull(index)) {
      return null;
    }

    List<Field> childFields = structVector.getField().getChildren();
    Map<String, Object> result = new LinkedHashMap<>(childFields.size());

    for (Field childField : childFields) {
      String fieldName = childField.getName();
      FieldVector childVector = structVector.getChild(fieldName);
      if (childVector != null) {
        Object childValue = extractValueFromVector(childVector, index, reader);
        result.put(fieldName, convertArrowTypeToPinotCompatible(childValue));
      }
    }

    return result;
  }

  /**
   * Extracts and decodes a Map value, handling dictionary-encoded keys and values.
   */
  @Nullable
  private Map<String, Object> extractMapValue(MapVector mapVector, int index, ArrowStreamReader reader) {
    if (mapVector.isNull(index)) {
      return null;
    }

    int startOffset = mapVector.getOffsetBuffer().getInt(index * MapVector.OFFSET_WIDTH);
    int endOffset = mapVector.getOffsetBuffer().getInt((index + 1) * MapVector.OFFSET_WIDTH);
    int entryCount = endOffset - startOffset;

    if (entryCount == 0) {
      return new LinkedHashMap<>();
    }

    StructVector entriesVector = (StructVector) mapVector.getDataVector();
    FieldVector keyVector = entriesVector.getChild(MapVector.KEY_NAME);
    FieldVector valueVector = entriesVector.getChild(MapVector.VALUE_NAME);

    Map<String, Object> result = new LinkedHashMap<>(entryCount);

    for (int i = 0; i < entryCount; i++) {
      int entryIndex = startOffset + i;
      Object key = extractValueFromVector(keyVector, entryIndex, reader);
      Object value = extractValueFromVector(valueVector, entryIndex, reader);

      Object convertedKey = convertArrowTypeToPinotCompatible(key);
      Object convertedValue = convertArrowTypeToPinotCompatible(value);
      result.put(String.valueOf(convertedKey), convertedValue);
    }

    return result;
  }

  /**
   * Converts Arrow-specific data types to Pinot-compatible types. This method handles the
   * incompatibility issues between Arrow's native data types and what Pinot expects.
   *
   * @param value The raw value from Arrow fieldVector.getObject()
   * @return A Pinot-compatible version of the value
   */
  @Nullable
  private Object convertArrowTypeToPinotCompatible(@Nullable Object value) {
    if (value == null) {
      return null;
    }

    // Handle nested List and Map values, including Arrow MapVector's representation
    if (value instanceof List) {
      List<?> originalList = (List<?>) value;
      if (!originalList.isEmpty()) {
        boolean looksLikeMapEntries = true;
        boolean sawNonNull = false;
        for (Object entryObj : originalList) {
          if (entryObj == null) {
            continue;
          }
          sawNonNull = true;
          if (!(entryObj instanceof Map)) {
            looksLikeMapEntries = false;
            break;
          }
          @SuppressWarnings("unchecked")
          Map<Object, Object> entryMap = (Map<Object, Object>) entryObj;
          if (!entryMap.containsKey(MapVector.KEY_NAME)) {
            looksLikeMapEntries = false;
            break;
          }
        }
        if (looksLikeMapEntries && sawNonNull) {
          Map<String, Object> flattened = new LinkedHashMap<>(originalList.size());
          for (Object entryObj : originalList) {
            if (entryObj == null) {
              continue;
            }
            @SuppressWarnings("unchecked")
            Map<Object, Object> entryMap = (Map<Object, Object>) entryObj;
            Object rawKey = entryMap.get(MapVector.KEY_NAME);
            Object rawVal = entryMap.get(MapVector.VALUE_NAME);
            Object convertedKey = convertArrowTypeToPinotCompatible(rawKey);
            Object convertedVal = convertArrowTypeToPinotCompatible(rawVal);
            flattened.put(String.valueOf(convertedKey), convertedVal);
          }
          return flattened;
        }
      }

      List<Object> convertedList = new ArrayList<>(originalList.size());
      for (Object element : originalList) {
        convertedList.add(convertArrowTypeToPinotCompatible(element));
      }
      return convertedList;
    }

    // Handle Arrow Text type -> String conversion
    if (value instanceof Text) {
      // Arrow VarCharVector.getObject() returns Text objects, but Pinot expects String
      return value.toString();
    }

    // Handle Arrow LocalDateTime -> java.sql.Timestamp conversion
    if (value instanceof LocalDateTime) {
      // Arrow TimeStampMilliVector.getObject() returns LocalDateTime, but Pinot expects
      // java.sql.Timestamp objects for proper timestamp handling and native support
      LocalDateTime dateTime = (LocalDateTime) value;
      return Timestamp.from(dateTime.toInstant(ZoneOffset.UTC));
    }

    // Handle other potential Arrow-specific types that might cause issues

    // For primitive types (Integer, Double, Boolean) and other Java standard types,
    // Arrow returns standard Java objects that are already Pinot-compatible
    return value;
  }
}
