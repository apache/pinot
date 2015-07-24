package com.linkedin.thirdeye.api;

import com.linkedin.thirdeye.impl.storage.IndexMetadata;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface StarTreeManager
{
  /**
   * @return
   *   A List of all collections for which configs have been registered.
   */
  Set<String> getCollections();

  /** @return a map of data directory to star tree index for a collection */
  Map<File, StarTree> getStarTrees(String collection);

  /**
   * Returns an in-memory star tree suitable for live updates.
   */
  StarTree getMutableStarTree(String collection);

  /**
   *
   * Returns the maxDataTime available in the collection
   */
  Long getMaxDataTime(String collection);

  /**
   * Returns the index metadata for a data segment.
   */
  IndexMetadata getIndexMetadata(UUID treeId);

  StarTreeConfig getConfig(String collection);

  /**
   * Restores a previously constructed tree.
   */
  void restore(File rootDir, String collection) throws Exception;

  /**
   * Closes all star trees this manager is managing
   */
  void close(String collection) throws IOException;

}
