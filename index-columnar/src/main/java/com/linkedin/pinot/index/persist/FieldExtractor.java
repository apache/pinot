package com.linkedin.pinot.index.persist;

import com.linkedin.pinot.index.data.GenericRow;
import com.linkedin.pinot.index.data.Schema;


/**
 * Take a GenericRow transform it to an indexable GenericRow.
 * Customized logic will apply in transform(...)
 *  
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public interface FieldExtractor {

  void setSchema(Schema schema);

  Schema getSchema();

  GenericRow transform(GenericRow row);
}
