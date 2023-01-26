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

import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.creator.TextIndexCreator;
import org.apache.pinot.spi.data.FieldSpec;


public abstract class AbstractTextIndexCreator implements TextIndexCreator {

  @Nullable
  private final Object _rawValueForTextIndex;

  /**
   * @param rawValueForTextIndex the value used as alternative (which must be an array when the field is multivalued) or
   *                             null if no alternative should be provided.
   */
  public AbstractTextIndexCreator(@Nullable Object rawValueForTextIndex, FieldSpec fieldSpec) {
    if (rawValueForTextIndex == null) {
      _rawValueForTextIndex = null;
    } else if (fieldSpec.isSingleValueField()) {
      _rawValueForTextIndex = rawValueForTextIndex;
    } else if (fieldSpec.getDataType().getStoredType() == FieldSpec.DataType.STRING) {
      _rawValueForTextIndex = new String[]{String.valueOf(rawValueForTextIndex)};
    } else if (fieldSpec.getDataType().getStoredType() == FieldSpec.DataType.BYTES) {
      _rawValueForTextIndex = new byte[][]{String.valueOf(rawValueForTextIndex).getBytes(StandardCharsets.UTF_8)};
    } else {
      throw new RuntimeException("Text Index is only supported for STRING and BYTES stored type");
    }
  }

  @Override
  public void addSingleValueCell(@Nonnull Object value, int dictId) {
    add((String) value);
  }

  @Override
  public void addMultiValueCell(@Nonnull Object[] values, @Nullable int[] dictIds) {
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

  @Nullable
  @Override
  public Object alternativeSingleValue(Object value) {
    return _rawValueForTextIndex;
  }

  @Nullable
  @Override
  public Object[] alternativeMultiValue(Object[] values) {
    return (Object[]) _rawValueForTextIndex;
  }
}
