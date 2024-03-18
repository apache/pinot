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
package org.apache.pinot.segment.spi.index.creator;

import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexCreator;


/**
 * Index creator for json index.
 */
public interface JsonIndexCreator extends IndexCreator {
  char KEY_VALUE_SEPARATOR = '\0';
  char KEY_VALUE_SEPARATOR_NEXT_CHAR = KEY_VALUE_SEPARATOR + 1;

  @Override
  default void add(@Nonnull Object value, int dictId)
      throws IOException {
    add((String) value);
  }

  @Override
  default void add(@Nonnull Object[] values, @Nullable int[] dictIds) {
  }

  /**
   * Adds the next json value.
   */
  void add(String jsonString)
      throws IOException;

  /**
   * Seals the index and flushes it to disk.
   */
  void seal()
      throws IOException;
}
