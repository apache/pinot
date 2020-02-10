package org.apache.pinot.tools.anonymizer;

import org.apache.pinot.core.segment.index.ColumnMetadata;


public interface GlobalDictionaries {

  String DICT_FILE_EXTENSION = ".dict";

  void addOrigValueToGlobalDictionary(Object origValue, String column, ColumnMetadata columnMetadata, int cardinality);

  void sortOriginalValuesInGlobalDictionaries();

  void addDerivedValuesToGlobalDictionaries();

  void serialize(String outputDir) throws Exception;

  Object getDerivedValueForOrigValueSV(String column, Object origValue);

  Object[] getDerivedValuesForOrigValuesMV(String column, Object[] origValues);
}
