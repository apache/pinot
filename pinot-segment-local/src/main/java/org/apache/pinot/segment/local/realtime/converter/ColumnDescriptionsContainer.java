package org.apache.pinot.segment.local.realtime.converter;

import java.util.List;


public class ColumnDescriptionsContainer {
  private final String _sortedColumn;
  private final List<String> _invertedIndexColumns;
  private final List<String> _textIndexColumns;
  private final List<String> _fstIndexColumns;
  private final List<String> _noDictionaryColumns;
  private final List<String> _varLengthDictionaryColumns;

  public ColumnDescriptionsContainer(String sortedColumn, List<String> invertedIndexColumns,
      List<String> textIndexColumns, List<String> fstIndexColumns, List<String> noDictionaryColumns,
      List<String> varLengthDictionaryColumns) {
    _sortedColumn = sortedColumn;
    _invertedIndexColumns = invertedIndexColumns;
    _textIndexColumns = textIndexColumns;
    _fstIndexColumns = fstIndexColumns;
    _noDictionaryColumns = noDictionaryColumns;
    _varLengthDictionaryColumns = varLengthDictionaryColumns;
  }

  public String getSortedColumn() {
    return _sortedColumn;
  }

  public List<String> getInvertedIndexColumns() {
    return _invertedIndexColumns;
  }

  public List<String> getTextIndexColumns() {
    return _textIndexColumns;
  }

  public List<String> getFstIndexColumns() {
    return _fstIndexColumns;
  }

  public List<String> getNoDictionaryColumns() {
    return _noDictionaryColumns;
  }

  public List<String> getVarLengthDictionaryColumns() {
    return _varLengthDictionaryColumns;
  }
}
