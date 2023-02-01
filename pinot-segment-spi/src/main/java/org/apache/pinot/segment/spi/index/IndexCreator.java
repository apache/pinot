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

package org.apache.pinot.segment.spi.index;

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * The interface used to create indexes.
 *
 * The lifecycle for an IndexCreator is to be created, receive one or more calls to either
 * {@link #addSingleValueCell(Object, int)} or {@link #addMultiValueCell(Object[], int[])} (but not
 * mix them),
 * a call to {@link #seal()} and finally be closed. Calls to add cell methods must be done in document id order,
 * starting from the first document id.
 */
public interface IndexCreator extends Closeable {
  /**
   * Adds the given single value cell to the index.
   *
   * Rows will be added in docId order, starting with the one with docId 0.
   *
   * @param value The nonnull value of the cell. In case the cell was actually null, a default value is received instead
   * @param dictId An optional dictionary value of the cell. If there is no dictionary, -1 is received
   */
  void addSingleValueCell(@Nonnull Object value, int dictId)
      throws IOException;

  /**
   * Adds the given multi value cell to the index
   *
   * Rows will be added in docId order, starting with the one with docId 0.
   *
   * @param values The nonnull value of the cell. In case the cell was actually null, an empty array is received instead
   * @param dictIds An optional array of dictionary values. If there is no dictionary, null is received.
   */
  void addMultiValueCell(@Nonnull Object[] values, @Nullable int[] dictIds)
      throws IOException;

  void seal()
      throws IOException;
}
