package com.linkedin.thirdeye.api;

import java.io.File;
import java.io.IOException;
import java.util.Set;

public interface StarTreeManager
{
  /**
   * @return
   *   A List of all collections for which configs have been registered.
   */
  Set<String> getCollections();

  /**
   * @return
   *  The StarTree for a collection.
   */
  StarTree getStarTree(String collection);

  /**
   * Restores a previously constructed tree.
   */
  void restore(File rootDir, String collection) throws Exception;

  /**
   * Removes and closes a star tree for a collection.
   */
  void remove(String collection) throws IOException;

  void open(String collection) throws IOException;

  /**
   * Closes all star trees this manager is managing
   */
  void close(String collection) throws IOException;
}
