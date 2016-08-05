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
package com.linkedin.pinot.core.segment.index;

import com.linkedin.pinot.common.utils.time.TimeUtils;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.TimeFieldSpec;

import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.TIME_UNIT;

public class ColumnMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(ColumnMetadata.class);

  private final String columnName;
  private final int cardinality;
  private final int totalRawDocs;
  private final int totalAggDocs;
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
  private final int totalNumberOfEntries;
  private final char paddingCharacter;

  public static ColumnMetadata fromPropertiesConfiguration(String column, PropertiesConfiguration config) {
    Builder builder = new Builder();
    builder.setColumnName(column);

    int cardinality =
        config.getInt(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.CARDINALITY));
    builder.setCardinality(cardinality);

    int totalDocs=
        config.getInt(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.TOTAL_DOCS));
    builder.setTotalDocs(totalDocs);

    final int totalRawDocs = config.getInt(
        V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.TOTAL_RAW_DOCS), totalDocs);
    builder.setTotalRawDocs(totalRawDocs);

    final int totalAggDocs = config
        .getInt(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.TOTAL_AGG_DOCS), 0);
    builder.setTotalAggDocs(totalAggDocs);

    final DataType dataType = DataType
        .valueOf(config
            .getString(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.DATA_TYPE)));
    builder.setDataType(dataType);

    final int bitsPerElement =
        config.getInt(
            V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.BITS_PER_ELEMENT));
    builder.setBitsPerElement(bitsPerElement);

    final int stringColumnMaxLength =
        config.getInt(
            V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.DICTIONARY_ELEMENT_SIZE));
    builder.setStringColumnMaxLength(stringColumnMaxLength);

    final FieldType fieldType = FieldType
        .valueOf(config
            .getString(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.COLUMN_TYPE))
            .toUpperCase());
    builder.setFieldType(fieldType);

    final boolean isSorted =
        config.getBoolean(V1Constants.MetadataKeys.Column
            .getKeyFor(column, V1Constants.MetadataKeys.Column.IS_SORTED));
    builder.setIsSorted(isSorted);

    final boolean hasInvertedIndex =
        config.getBoolean(V1Constants.MetadataKeys.Column
            .getKeyFor(column, V1Constants.MetadataKeys.Column.HAS_INVERTED_INDEX));
    builder.setHasInvertedIndex(hasInvertedIndex);

    final boolean isSingleValue =
        config.getBoolean(
            V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.IS_SINGLE_VALUED));
    builder.setInSingleValue(isSingleValue);

    final int maxNumberOfMultiValues =
        config.getInt(
            V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.MAX_MULTI_VALUE_ELEMTS));
    builder.setMaxNumberOfMultiValues(maxNumberOfMultiValues);

    final boolean hasNulls =
        config.getBoolean(V1Constants.MetadataKeys.Column
            .getKeyFor(column, V1Constants.MetadataKeys.Column.HAS_NULL_VALUE));
    builder.setContainsNulls(hasNulls);

    TimeUnit segmentTimeUnit = TimeUnit.DAYS;
    if (config
        .containsKey(V1Constants.MetadataKeys.Segment.TIME_UNIT)) {
      segmentTimeUnit = TimeUtils
          .timeUnitFromString(config.getString(TIME_UNIT));
    }
    builder.setTimeunit(segmentTimeUnit);

    final boolean hasDictionary =
        config.getBoolean(V1Constants.MetadataKeys.Column
            .getKeyFor(column, V1Constants.MetadataKeys.Column.HAS_DICTIONARY), true);
    builder.setHasDictionary(hasDictionary);

    final int totalNumberOfEntries =
        config.getInt(V1Constants.MetadataKeys.Column
            .getKeyFor(column, V1Constants.MetadataKeys.Column.TOTAL_NUMBER_OF_ENTRIES));
    builder.setTotalNumberOfEntries(totalNumberOfEntries);

    char paddingCharacter = V1Constants.Str.DEFAULT_STRING_PAD_CHAR;
    if (config
        .containsKey(V1Constants.MetadataKeys.Segment.SEGMENT_PADDING_CHARACTER)) {
      String padding = config.getString(V1Constants.MetadataKeys.Segment.SEGMENT_PADDING_CHARACTER);
      paddingCharacter = StringEscapeUtils.unescapeJava(padding).charAt(0);
    } else {
      paddingCharacter = V1Constants.Str.LEGACY_STRING_PAD_CHAR;
    }

    builder.setPaddingCharacter(paddingCharacter);

    return builder.build();
  }

  public static class Builder {
    private String columnName;
    private int cardinality;
    private int totalDocs;
    private DataType dataType;
    private int bitsPerElement;
    private int stringColumnMaxLength;
    private FieldType fieldType;
    private boolean isSorted;
    private boolean hasInvertedIndex;
    private boolean inSingleValue;
    private int maxNumberOfMultiValues;
    private boolean containsNulls;
    private TimeUnit timeunit;
    private boolean hasDictionary;
    private int totalNumberOfEntries;
    private int totalRawDocs;
    private int totalAggDocs;
    private char paddingCharacter;

    public Builder setColumnName(String columnName) {
      this.columnName = columnName;
      return this;
    }

    public Builder setCardinality(int cardinality) {
      this.cardinality = cardinality;
      return this;
    }

    public Builder setTotalDocs(int totalDocs) {
      this.totalDocs = totalDocs;
      return this;
    }

    public Builder setDataType(DataType dataType) {
      this.dataType = dataType;
      return this;
    }

    public Builder setBitsPerElement(int bitsPerElement) {
      this.bitsPerElement = bitsPerElement;
      return this;
    }

    public Builder setStringColumnMaxLength(int stringColumnMaxLength) {
      this.stringColumnMaxLength = stringColumnMaxLength;
      return this;
    }

    public Builder setFieldType(FieldType fieldType) {
      this.fieldType = fieldType;
      return this;
    }

    public Builder setIsSorted(boolean isSorted) {
      this.isSorted = isSorted;
      return this;
    }

    public Builder setHasInvertedIndex(boolean hasInvertedIndex) {
      this.hasInvertedIndex = hasInvertedIndex;
      return this;
    }

    public Builder setInSingleValue(boolean inSingleValue) {
      this.inSingleValue = inSingleValue;
      return this;
    }

    public Builder setMaxNumberOfMultiValues(int maxNumberOfMultiValues) {
      this.maxNumberOfMultiValues = maxNumberOfMultiValues;
      return this;
    }

    public Builder setContainsNulls(boolean containsNulls) {
      this.containsNulls = containsNulls;
      return this;
    }

    public Builder setTimeunit(TimeUnit timeunit) {
      this.timeunit = timeunit;
      return this;
    }

    public Builder setHasDictionary(boolean hasDictionary) {
      this.hasDictionary = hasDictionary;
      return this;
    }

    public Builder setTotalNumberOfEntries(int totalNumberOfEntries) {
      this.totalNumberOfEntries = totalNumberOfEntries;
      return this;
    }

    public Builder setPaddingCharacter(char paddingCharacter) {
      this.paddingCharacter = paddingCharacter;
      return this;
    }

    public Builder setTotalRawDocs(int totalRawDocs) {
      this.totalRawDocs = totalRawDocs;
      return this;
    }

    public Builder setTotalAggDocs(int totalAggDocs) {
      this.totalAggDocs = totalAggDocs;
      return this;
    }

    public ColumnMetadata build() {
      return new ColumnMetadata(columnName, cardinality, totalRawDocs, totalAggDocs,totalDocs, dataType, bitsPerElement,
      stringColumnMaxLength, fieldType, isSorted, hasInvertedIndex,
      inSingleValue, maxNumberOfMultiValues, containsNulls, hasDictionary, timeunit,
      totalNumberOfEntries, paddingCharacter);
    }


  }

  public static  ColumnMetadata.Builder newBuilder() {
    return new ColumnMetadata.Builder();
  }

  private ColumnMetadata(String columnName, int cardinality, int totalRawDocs, int totalAggDocs, int totalDocs, DataType dataType, int bitsPerElement,
      int stringColumnMaxLength, FieldType fieldType, boolean isSorted, boolean hasInvertedIndex,
      boolean insSingleValue, int maxNumberOfMultiValues, boolean hasNulls, boolean hasDictionary, TimeUnit timeunit,
      int totalNumberOfEntries, char paddingCharacter) {

    this.columnName = columnName;
    this.cardinality = cardinality;
    this.totalRawDocs = totalRawDocs;
    this.totalAggDocs = totalAggDocs;
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
    this.totalNumberOfEntries = totalNumberOfEntries;
    this.paddingCharacter = paddingCharacter;
  }

  public String getColumnName() {
    return columnName;
  }

  public int getTotalNumberOfEntries() {
    return totalNumberOfEntries;
  }

  public char getPaddingCharacter() {
    return paddingCharacter;
  }

  public int getMaxNumberOfMultiValues() {
    return maxNumberOfMultiValues;
  }

  public int getCardinality() {
    return cardinality;
  }

  public int getTotalRawDocs() {
    return totalRawDocs;
  }

  public int getTotalDocs() {
    return totalDocs;
  }

  public int getTotalAggreateDocs() {
    return totalAggDocs;
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

  public TimeUnit getTimeunit() { return timeunit; }
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
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Unable to access field " + field, ex);
        }
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
