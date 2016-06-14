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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;


public class StarTree implements Serializable {

  private StarTreeIndexNode _root;
  private List<String> _dimensionList;

  public StarTree(StarTreeIndexNode root, List<String> dimensionList) {
    _root = root;
    _dimensionList = dimensionList;
  }

  public StarTreeIndexNode getRoot() {
    return _root;
  }

  public List<String> getDimensionList() {
    return _dimensionList;
  }

  public void writeTree(OutputStream outputStream) throws IOException {
    ObjectOutputStream oos = new ObjectOutputStream(outputStream);
    oos.writeObject(this);
  }

  /**
   * De-serializes a StarTree structure generated with {@link #writeTree(OutputStream)}.
   */
  public static StarTree readTree(InputStream inputStream) throws IOException, ClassNotFoundException {
    ObjectInputStream ois = new ObjectInputStream(inputStream);
    return (StarTree) ois.readObject();
  }
}
