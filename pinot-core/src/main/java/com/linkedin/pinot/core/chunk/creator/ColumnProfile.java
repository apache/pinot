package com.linkedin.pinot.core.chunk.creator;

import java.util.Set;

import com.linkedin.pinot.common.data.FieldSpec.DataType;

/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 6, 2014
 */

public interface ColumnProfile<T> {

  public DataType getDataType();

  public T getMinValue();

  public T getMaxValue();

  public int getCardinality();

  public Set<T> getSortedListOfUniqueValues();
}
