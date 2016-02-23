/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

import com.google.common.collect.HashBiMap;


public class StarTree implements Serializable{

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  StarTreeIndexNode root;
  private HashBiMap<String, Integer> dimensionNameToIndexMap;

  public StarTree(StarTreeIndexNode root, HashBiMap<String, Integer> dimensionNameToIndexMap) {
    this.root = root;
    this.dimensionNameToIndexMap = dimensionNameToIndexMap;
  }

  public StarTreeIndexNode getRoot() {
    return root;
  }

  public HashBiMap<String, Integer> getDimensionNameToIndexMap() {
    return dimensionNameToIndexMap;
  }

  /**
   * Returns a Java-serialized StarTree structure of this node and all its sub-trees.
   */
  public byte[] toBytes() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    writeTree(baos);
    baos.close();
    return baos.toByteArray();
  }

  public void writeTree(OutputStream outputStream) throws IOException {
    ObjectOutputStream oos = new ObjectOutputStream(outputStream);
    oos.writeObject(this);
  }

  /**
   * De-serializes a StarTree structure generated with {@link #toBytes}.
   */
  public static StarTree fromBytes(InputStream inputStream) throws IOException, ClassNotFoundException {
    ObjectInputStream ois = new ObjectInputStream(inputStream);
    StarTree node = (StarTree) ois.readObject();
    return node;
  }

}
