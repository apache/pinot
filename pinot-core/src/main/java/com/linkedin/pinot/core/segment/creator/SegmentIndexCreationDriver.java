package com.linkedin.pinot.core.segment.creator;

import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 6, 2014
 */

public interface SegmentIndexCreationDriver {

  /**
   * This is where you pass in generator configuration,
   * the driver takes care of the rest
   * @param config
   */
  public void init(SegmentGeneratorConfig config) throws Exception;

  /**
   * When build is called, the following should happen
   * extract data from input files
   * profile each column
   * create indexes
   */
  public void build() throws Exception;

}
