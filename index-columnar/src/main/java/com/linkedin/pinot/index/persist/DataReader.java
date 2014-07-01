package com.linkedin.pinot.index.persist;

import com.linkedin.pinot.index.data.GenericRow;
import com.linkedin.pinot.index.data.Schema;


/**
 * Read data file and shape raw data format to GenericRow.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public interface DataReader {
  public void init() throws Exception;

  public boolean hasNext();

  public Schema getSchema();

  public GenericRow getNextIndexableRow();

  public void close() throws Exception;
}
