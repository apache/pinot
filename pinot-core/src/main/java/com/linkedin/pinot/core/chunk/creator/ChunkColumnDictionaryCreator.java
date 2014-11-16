package com.linkedin.pinot.core.chunk.creator;

/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 6, 2014
 */

public interface ChunkColumnDictionaryCreator<T> {

  public void init();

  public void index(T value);

  public void seal();

}
