package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.Schema;


/**
 * Read data file and shape raw data format to GenericRow.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */

public interface RecordReader {
  public void init() throws Exception;

  public void rewind() throws Exception;

  public boolean hasNext();

  public Schema getSchema();

  public GenericRow next();

  public void close() throws Exception;
}
