package org.apache.pinot.query.planner.partitioning;

import java.io.Serializable;


public class FieldSelectionKeySelector implements KeySelector<Object[], Object>, Serializable {

  private int _columnIndex;

  public FieldSelectionKeySelector(int columnIndex) {
    _columnIndex = columnIndex;
  }

  @Override
  public Object getKey(Object[] input) {
    return input[_columnIndex];
  }
}
