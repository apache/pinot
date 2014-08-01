package com.linkedin.pinot.core.indexsegment.tools;

import javax.swing.table.AbstractTableModel;

import com.linkedin.pinot.core.indexsegment.utils.SortedIntArray;


public class SortedForwardIndexTable extends AbstractTableModel {
  SortedIntArray sortedIntArray;
  private String[] columnNames = { "Dictionary Id", "Min Doc Id", "Max Doc Id" };

  public SortedForwardIndexTable(SortedIntArray intArray) {
    sortedIntArray = intArray;
  }

  public String getColumnName(int col) {
    return columnNames[col];
  }

  @Override
  public int getRowCount() {
    return sortedIntArray.size();
  }

  @Override
  public int getColumnCount() {
    return columnNames.length;
  }

  @Override
  public Object getValueAt(int rowIndex, int columnIndex) {
    if (columnIndex == 0) {
      return rowIndex;
    }
    if (columnIndex == 1) {
      return sortedIntArray.getMinDocId(rowIndex);
    }
    return sortedIntArray.getMaxDocId(rowIndex);
  }

}
