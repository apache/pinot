package com.linkedin.pinot.core.indexsegment.tools;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.swing.table.AbstractTableModel;

import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentMetadata;


public class MetadataTable extends AbstractTableModel {
  ColumnarSegmentMetadata metadata;
  Map<String, String> metadataMap;
  List<Object> list;
  private String[] columnNames = { "Key", "Value" };

  @SuppressWarnings("unchecked")
  public MetadataTable(ColumnarSegmentMetadata m) {
    metadata = m;
    Iterator<String> it = metadata.getKeys();
    metadataMap = new HashMap<String, String>();
    while (it.hasNext()) {
      String key = it.next();
      metadataMap.put(key, metadata.getString(key));
    }
    metadataMap.entrySet().toArray();
    list = Arrays.asList(metadataMap.entrySet().toArray());
  }

  @Override
  public int getRowCount() {
    return metadataMap.size();
  }

  public String getColumnName(int col) {
    return columnNames[col];
  }

  @Override
  public int getColumnCount() {
    return 2;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object getValueAt(int rowIndex, int columnIndex) {
    Entry<String, String> e = (Entry<String, String>) list.get(rowIndex);
    if (columnIndex == 0) {

      return e.getKey();
    }
    return e.getValue();
  }

}
