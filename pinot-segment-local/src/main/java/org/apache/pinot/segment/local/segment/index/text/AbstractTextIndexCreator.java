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

package org.apache.pinot.segment.local.segment.index.text;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.creator.TextIndexCreator;


public abstract class AbstractTextIndexCreator implements TextIndexCreator {

  @Override
  public void add(@Nonnull Object value, int dictId) {
    add((String) value);
  }

  @Override
  public void add(@Nonnull Object[] values, @Nullable int[] dictIds) {
    int length = values.length;
    if (values instanceof String[]) {
      add((String[]) values, length);
    } else {
      String[] strings = new String[length];
      for (int i = 0; i < length; i++) {
        strings[i] = (String) values[i];
      }
      add(strings, length);
    }
  }
}
