package com.linkedin.pinot.core.segment.creator;

import com.linkedin.pinot.core.data.GenericRow;

/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 6, 2014
 */

public interface SegmentPreIndexStatsCollector {

  public void init();

  public void build() throws Exception;

  public AbstractColumnPreIndexStatsCollector getColumnProfileFor(String column) throws Exception;

  void collectRow(GenericRow row) throws Exception;

  void logStats();
}
