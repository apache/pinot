package com.linkedin.pinot.raw.record.readers;

import com.linkedin.pinot.index.data.GenericRow;
import com.linkedin.pinot.index.data.Schema;


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
