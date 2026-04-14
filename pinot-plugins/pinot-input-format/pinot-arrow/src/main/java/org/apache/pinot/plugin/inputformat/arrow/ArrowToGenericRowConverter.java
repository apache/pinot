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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.ipc.ArrowReader;
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

  @Nullable
  private final Set<String> _fieldsToRead;

  /** Default constructor that processes all fields from Arrow batch. */
  public ArrowToGenericRowConverter() {
    this(null);
  }

  /**
   * Constructor that processes only specified fields from Arrow batch.
   *
   * @param fieldsToRead Set of field names to read. If null or empty, reads all fields.
   */
  public ArrowToGenericRowConverter(@Nullable Set<String> fieldsToRead) {
    _fieldsToRead = (fieldsToRead == null || fieldsToRead.isEmpty()) ? null : Set.copyOf(fieldsToRead);
  }

  /**
   * Converts an Arrow VectorSchemaRoot to a Pinot {@code GenericRow}. Processes ALL rows from the
   * Arrow batch and stores them as a list using MULTIPLE_RECORDS_KEY.
   *
   * @param reader ArrowReader containing the data
   * @param root Arrow VectorSchemaRoot containing the data
   * @param destination Optional destination {@code GenericRow}, will create new if null
   * @return {@code GenericRow} containing {@code List<GenericRow>} with all converted rows, or null
   *     if no data available
   */
  @Nullable
  public GenericRow convert(
      ArrowReader reader, VectorSchemaRoot root, GenericRow destination) {
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
   * @param reader ArrowReader containing the data
   * @param root Arrow VectorSchemaRoot containing the data
   * @param rowIndex Index of the row to convert (0-based)
   * @return {@code GenericRow} with converted data, or null if row index is invalid
   */
  @Nullable
  public GenericRow convertSingleRow(
      ArrowReader reader, VectorSchemaRoot root, int rowIndex) {
    int rowCount = root.getRowCount();
    if (rowIndex < 0 || rowIndex >= rowCount) {
      logger.warn("Row index {} is out of bounds [0, {}) for Arrow batch", rowIndex, rowCount);
      return null;
    }
    return convertSingleRow(reader, root, rowIndex, new GenericRow());
  }

  /**
   * Converts a single row from Arrow VectorSchemaRoot into the given {@code GenericRow}.
   *
   * @param reader ArrowReader containing the data
   * @param root Arrow VectorSchemaRoot containing the data
   * @param rowIndex Index of the row to convert (0-based)
   * @param reuse GenericRow to populate with converted data
   * @return the populated {@code GenericRow}
   */
  public GenericRow convertSingleRow(
      ArrowReader reader, VectorSchemaRoot root, int rowIndex, GenericRow reuse) {
    reuse.clear();
    int convertedFields = 0;

    // Process all fields in the Arrow schema
    for (int i = 0; i < root.getFieldVectors().size(); i++) {
      Object value;

      FieldVector fieldVector = root.getFieldVectors().get(i);
      String fieldName = fieldVector.getField().getName();
      if (_fieldsToRead != null && !_fieldsToRead.contains(fieldName)) {
        continue;
      }
      try {
        if (fieldVector.getField().getDictionary() != null) {
          long dictionaryId = fieldVector.getField().getDictionary().getId();
          try (ValueVector realFieldVector =
              DictionaryEncoder.decode(
                  fieldVector, reader.getDictionaryVectors().get(dictionaryId))) {
            value = realFieldVector.getObject(rowIndex);
          }
        } else {
          value = fieldVector.getObject(rowIndex);
        }
        if (value != null) {
          // Convert Arrow-specific types to Pinot-compatible types
          Object pinotCompatibleValue = convertArrowTypeToPinotCompatible(value);
          reuse.putValue(fieldName, pinotCompatibleValue);
          convertedFields++;
        } else {
          reuse.putValue(fieldName, null);
        }
      } catch (Exception e) {
        logger.error("Error extracting value for field: {} at row {}", fieldName, rowIndex, e);
      }
    }

    logger.debug("Converted {} fields from Arrow row {} to GenericRow", convertedFields, rowIndex);
    return reuse;
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
