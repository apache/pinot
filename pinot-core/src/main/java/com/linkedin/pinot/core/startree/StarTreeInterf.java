/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.startree;

import com.google.common.collect.HashBiMap;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;


/**
 * Interface for StarTree.
 */
public interface StarTreeInterf extends Serializable {

  enum Version {
    V1,
    V2
  }

  /**
   * Returns the root of the StarTree.
   * @return
   */
  StarTreeIndexNodeInterf getRoot();

  /**
   * Returns the version of star tree.
   * @return
   */
  Version getVersion();

  /**
   * Returns the total number of nodes in the star tree.
   */
  int getNumNodes();

  /**
   * Returns a bi-map of mapping between dimension name and
   * its index.
   * @return
   */
  HashBiMap<String, Integer> getDimensionNameToIndexMap();

  /**
   * Serializes and writes the StarTree on to the provided file.
   * @param outputFile
   */
  void writeTree(File outputFile)
      throws IOException;

  /**
   * Print the tree.
   */
  void printTree();
}
