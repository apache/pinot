package com.linkedin.pinot.common.utils.time;

import org.joda.time.DateTime;

public interface TimeConverter {
  /**
   *
   * @param incoming
   * @return
   */
  public long convert(Object incoming);

  /**
   *
   * @param incoming
   * @return
   */
  public DateTime getDataTimeFrom(long incoming);
}
