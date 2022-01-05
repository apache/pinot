package org.apache.pinot.query.planner.partitioning;

public class FieldSelectionKeySelector implements KeySelector<Object[], Object> {

  private int _columnIndex;

  public FieldSelectionKeySelector(int columnIndex) {
    _columnIndex = columnIndex;
  }

  @Override
  public Object getKey(Object[] input) {
    return input[_columnIndex];
  }
}
