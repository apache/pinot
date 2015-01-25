package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;


/**
 * Generic interface to implement any new file format in which
 * input data can be read and converted into segments.
 *
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */

public interface RecordReader {

  /**
   *
   * @throws Exception
   */
  public void init() throws Exception;

  /**
   * Rewind is called in case we need to iterate through
   * the input data again.. Once such case would be when
   * we need to create a dictionary. Cannot call this if
   * close is called first
   *
   * @throws Exception
   */
  public void rewind() throws Exception;

  /**
   *
   * @return
   */
  public boolean hasNext();

  /**
   *
   * @return
   */
  public Schema getSchema();

  /**
   *
   * @return
   */
  public GenericRow next();

  /**
   *
   * @throws Exception
   */
  public void close() throws Exception;
}
