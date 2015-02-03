package com.linkedin.pinot.core.realtime;

import com.linkedin.pinot.core.data.GenericRow;

public interface StreamProvider {

  /**
   *
   */
  public void init(StreamProviderConfig streamProviderConfig);

  /**
   * return GenericRow
   */
  public GenericRow next();

  /**
   *
   * @param offset
   * @return
   */
  public GenericRow next(long offset);

  /**
   *
   * @return
   */
  public long currentOffset();

  /**
   *
   */
  public void commit();

  /**
   *
   * @param offset
   */
  public void commit(long offset);

  /**
   *
   */
  public void shutdown();

}
