package com.linkedin.pinot.core.chunk.index;


import java.lang.reflect.Field;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 12, 2014
 */

public class ChunkColumnMetadata {
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

  public ChunkColumnMetadata(String columnName,int cardinality, int totalDocs, DataType dataType, int bitsPerElement, int stringColumnMaxLength,
      FieldType fieldType, boolean isSorted, boolean hasInvertedIndex, boolean insSingleValue, int maxNumberOfMultiValues) {
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
    return new FieldSpec(columnName, fieldType, dataType, inSingleValue);
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    final String newLine = System.getProperty("line.separator");

    result.append( this.getClass().getName() );
    result.append( " Object {" );
    result.append(newLine);

    //determine fields declared in this class only (no fields of superclass)
    final Field[] fields = this.getClass().getDeclaredFields();

    //print field names paired with their values
    for ( final Field field : fields  ) {
      result.append("  ");
      try {
        result.append( field.getName() );
        result.append(": ");
        //requires access to private field:
        result.append( field.get(this) );
      } catch ( final IllegalAccessException ex ) {
        System.out.println(ex);
      }
      result.append(newLine);
    }
    result.append("}");

    return result.toString();
  }

}
