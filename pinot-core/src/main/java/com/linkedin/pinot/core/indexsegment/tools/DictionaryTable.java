package com.linkedin.pinot.core.indexsegment.tools;

import javax.swing.table.AbstractTableModel;

import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;


public class DictionaryTable extends AbstractTableModel {
  private Dictionary<?> dict;
  private String[] columnNames = { "Index", "Raw Value" };

  public DictionaryTable(Dictionary<?> dictionary) {
    dict = dictionary;
  }

  @Override
  public int getRowCount() {
    return dict.size();
  }

  public String getColumnName(int col) {
    return columnNames[col];
  }

  @Override
  public int getColumnCount() {
    return 2;
  }

  @Override
  public Object getValueAt(int rowIndex, int columnIndex) {
    if (columnIndex == 0) {
      return rowIndex;
    }
    return dict.getString(rowIndex);
  }

}
