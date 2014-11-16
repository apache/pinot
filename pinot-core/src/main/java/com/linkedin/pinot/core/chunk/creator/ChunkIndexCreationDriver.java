package com.linkedin.pinot.core.chunk.creator;

import com.linkedin.pinot.core.indexsegment.generator.ChunkGeneratorConfiguration;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 6, 2014
 */

public interface ChunkIndexCreationDriver {

  /**
   * This is where you pass in generator configuration,
   * the driver takes care of the rest
   * @param config
   */
  public void init(ChunkGeneratorConfiguration config) throws Exception;

  /**
   * When build is called, the following should happen
   * extract data from input files
   * profile each column
   * create indexes
   */
  public void build() throws Exception;

}
