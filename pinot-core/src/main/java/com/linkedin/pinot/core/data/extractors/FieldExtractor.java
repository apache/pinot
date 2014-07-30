package com.linkedin.pinot.core.data.extractors;

import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.Schema;


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
