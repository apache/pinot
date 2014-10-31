package com.linkedin.thirdeye.api;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;

public interface StarTree
{
  StarTreeConfig getConfig();

  /**
   * Given a query consisting of specific dimension values, searches the tree for the corresponding
   * aggregate metrics.
   *
   * @param query
   *  A query with fixed and/or "*" values for each dimension
   * @return
   *  The representative record containing aggregate metrics.
   */
  StarTreeRecord search(StarTreeQuery query);

  /**
   * Adds a record to the tree.
   */
  void add(StarTreeRecord record);

  /**
   * Opens all resources at leaves
   */
  void open() throws IOException;

  /**
   * Closes all resources at leaves
   */
  void close() throws IOException;

  /**
   * Writes the tree structure (not leaf data) to output stream
   */
  void save(OutputStream outputStream) throws IOException;

  String toString(boolean includeRecords);

  /**
   * @return
   *  A set of every observed value for a dimension in the tree
   */
  Set<String> getDimensionValues(String dimensionName);

  /**
   * @return
   *  The set of observed values that have been (or will have been) rolled up into "other"
   */
  Set<String> getOtherDimensionValues(String dimensionName);

  /**
   * @return
   *  The root node in the tree
   */
  StarTreeNode getRoot();
}
