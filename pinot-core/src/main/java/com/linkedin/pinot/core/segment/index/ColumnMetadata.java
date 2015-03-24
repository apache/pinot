/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.index;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.TimeFieldSpec;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 12, 2014
 */

public class ColumnMetadata {
  private final String columnName;
  private final int cardinality;
  private final int totalDocs;
  private final DataType dataType;
  private final int bitsPerElement;
  private final int stringColumnMaxLength;
  private final FieldType fieldType;
  private final boolean isSorted;
  private final boolean hasInvertedIndex;
  private final boolean inSingleValue;
  private final int maxNumberOfMultiValues;
  private final boolean containsNulls;
  private final TimeUnit timeunit;
  private final boolean hasDictionary;



  public ColumnMetadata(String columnName, int cardinality, int totalDocs, DataType dataType, int bitsPerElement, int stringColumnMaxLength,
      FieldType fieldType, boolean isSorted, boolean hasInvertedIndex, boolean insSingleValue, int maxNumberOfMultiValues, boolean hasNulls, boolean hasDictionary, TimeUnit timeunit) {

    this.columnName = columnName;
    this.cardinality = cardinality;
    this.totalDocs = totalDocs;
    this.dataType = dataType;
    this.bitsPerElement = bitsPerElement;
    this.stringColumnMaxLength = stringColumnMaxLength;
    this.fieldType = fieldType;
    this.isSorted = isSorted;
    this.hasInvertedIndex = hasInvertedIndex;
    inSingleValue = insSingleValue;
    this.maxNumberOfMultiValues = maxNumberOfMultiValues;
    this.containsNulls = hasNulls;
    this.timeunit = timeunit;
    this.hasDictionary = hasDictionary;
  }

  public int getMaxNumberOfMultiValues() {
    return maxNumberOfMultiValues;
  }

  public int getCardinality() {
    return cardinality;
  }

  public int getTotalDocs() {
    return totalDocs;
  }

  public DataType getDataType() {
    return dataType;
  }

  public int getBitsPerElement() {
    return bitsPerElement;
  }

  public int getStringColumnMaxLength() {
    return stringColumnMaxLength;
  }

  public FieldType getFieldType() {
    return fieldType;
  }

  public boolean isSorted() {
    return isSorted;
  }

  public boolean isHasInvertedIndex() {
    return hasInvertedIndex;
  }

  public boolean isSingleValue() {
    return inSingleValue;
  }

  public FieldSpec toFieldSpec() {
    switch (fieldType) {
      case DIMENSION:
        return new DimensionFieldSpec(columnName, dataType, inSingleValue);
      case TIME:
        return new TimeFieldSpec(columnName, dataType, timeunit);
      case METRIC:
        return new MetricFieldSpec(columnName, dataType);
    }
    return null;
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    final String newLine = System.getProperty("line.separator");

    result.append(this.getClass().getName());
    result.append(" Object {");
    result.append(newLine);

    //determine fields declared in this class only (no fields of superclass)
    final Field[] fields = this.getClass().getDeclaredFields();

    //print field names paired with their values
    for (final Field field : fields) {
      result.append("  ");
      try {
        result.append(field.getName());
        result.append(": ");
        //requires access to private field:
        result.append(field.get(this));
      } catch (final IllegalAccessException ex) {
        result.append("[ERROR]");
      }
      result.append(newLine);
    }
    result.append("}");

    return result.toString();
  }

  public boolean hasNulls() {
    return containsNulls;
  }

  public boolean hasDictionary() {
    return hasDictionary;
  }
}
