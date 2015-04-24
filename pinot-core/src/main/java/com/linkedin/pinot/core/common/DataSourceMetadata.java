package com.linkedin.pinot.core.common;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;


public interface DataSourceMetadata {

  public DataType getDataType();

  public boolean hasInvertedIndex();

  public boolean isSorted();

  public boolean hasDictionary();

  public int cardinality();

  public FieldType getFieldType();

  public boolean isSingleValue();
}
