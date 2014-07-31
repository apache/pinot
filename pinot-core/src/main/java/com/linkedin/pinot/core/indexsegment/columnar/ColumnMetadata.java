package com.linkedin.pinot.core.indexsegment.columnar;

import java.io.File;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;


/**
 * Jul 15, 2014
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */
public class ColumnMetadata extends PropertiesConfiguration {

  private String name;
  private int dictionarySize;
  private FieldType fieldType;

  public ColumnMetadata(File metaFile, String column, FieldType type) throws ConfigurationException {
    super(metaFile);
    this.fieldType = type;
    this.name = column;
  }

  public boolean isSorted() {
    return getBoolean(V1Constants.MetadataKeys.Column.getKeyFor(name, V1Constants.MetadataKeys.Column.IS_SORTED));
  }

  public boolean isSingleValued() {
    return getBoolean(V1Constants.MetadataKeys.Column.getKeyFor(name, V1Constants.MetadataKeys.Column.IS_SINGLE_VALUED));
  }


  public String getName() {
    return name;
  }

  public int getDictionarySize() {
    return Integer.parseInt(getString(V1Constants.MetadataKeys.Column.getKeyFor(name,
        V1Constants.MetadataKeys.Column.CARDINALITY)));
  }

  public int getPerElementSizeForStringDictionary() {
    return Integer.parseInt(getString(V1Constants.MetadataKeys.Column.getKeyFor(name,
        V1Constants.MetadataKeys.Column.DICTIONARY_ELEMENT_SIZE)));
  }

  public DataType getDataType() {
    return DataType.valueOf(getString(V1Constants.MetadataKeys.Column.getKeyFor(name,
        V1Constants.MetadataKeys.Column.DATA_TYPE)));
  }

  public FieldType getFieldType() {
    return fieldType;
  }

  public int getBitsPerElementInForwardIndex() {
    return Integer.parseInt(getString(V1Constants.MetadataKeys.Column.getKeyFor(name,
        V1Constants.MetadataKeys.Column.BITS_PER_ELEMENT)));
  }

  public int getTotalDocs() {
    return Integer.parseInt(getString(V1Constants.MetadataKeys.Column.getKeyFor(name,
        V1Constants.MetadataKeys.Column.TOTAL_DOCS)));
  }

}
