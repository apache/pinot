/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.segment.local.utils.nativefst.mutablefst;


/**
 * A mutable FST represents a FST which can have arbitrary inputs added to it at
 * any given point of time. Unlike a normal FST which is build once read many, mutable
 * FST can be concurrently written to and read from. Mutable FST provides real time search
 * i.e. search will see words as they are added without needing a flush.
 *
 * Unlike a normal FST, mutable FST does not require the entire input beforehand nor
 * does it require the input to be sorted. Single word additions work well with
 * mutable FST.
 *
 * The reason as to why normal FST and mutable FST have different interfaces is because
 * normal i.e. immutable FST is optimized for storage and represents arcs, nodes and labels
 * by offsets. Thus, all operations are done in integer offsets.
 *
 * Unlike a normal FST, mutable FST has all components mutable i.e. arc and node. Thus, a
 * mutable FST operates on different data structures and follows a slightly different
 * interface.
 *
 * However, functionality wise, the interfaces for immutable FST and mutable FST are
 * identical with no difference.
 */
public interface MutableFST {

  /**
   * The start state in the FST; there must be exactly one
   * @return
   */
  MutableState getStartState();

  /**
   * Set the start state
   */
  void setStartState(MutableState mutableState);

  /**
   * throws an exception if the FST is constructed in an invalid state
   */
  void throwIfInvalid();

  /**
   * Add a path to the FST
   */
  void addPath(String word, int outputSymbol);
}
