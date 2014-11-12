package com.linkedin.thirdeye.api;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Set;

public interface StarTreeManager
{
  /**
   * @return
   *   A List of all collections for which configs have been registered.
   */
  Set<String> getCollections();

  /**
   * Registers a config for a collection.
   *
   * <p>
   *   A config needs to be registered in order to use a StarTree for a collection.
   * </p>
   *
   * <p>
   *   A config can only be registered once. Subsequent registrations for the same collection will fail.
   * </p>
   */
  void registerConfig(String collection, StarTreeConfig config);

  /**
   * @return
   *  The configuration for a collection
   */
  StarTreeConfig getConfig(String collection);

  /**
   * Removes the registered config for a collection
   */
  void removeConfig(String collection);

  /**
   * @return
   *  The StarTree for a collection.
   */
  StarTree getStarTree(String collection);

  /**
   * Loads a stream of records into the StarTree for a given collection.
   *
   * <p>
   *   N.b. for best results, the records should have high entropy w.r.t. dimensional values in the stream.
   *   This is because splits in the tree will occur as records are loaded, and we want to ensure that at
   *   time of split, the records we analyze for things like dimensional cardinality are a representative
   *   sample of the population.
   * </p>
   */
  void load(String collection, Iterable<StarTreeRecord> records) throws IOException;

  /**
   * Restores a previously constructed tree.
   */
  void restore(File rootDir, String collection) throws Exception;

  /**
   * Removes and closes a star tree for a collection.
   */
  void remove(String collection) throws IOException;
}
