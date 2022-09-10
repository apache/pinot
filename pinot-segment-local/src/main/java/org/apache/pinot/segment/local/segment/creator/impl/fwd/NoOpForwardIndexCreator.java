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
package org.apache.pinot.segment.local.segment.creator.impl.fwd;

import java.io.IOException;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Forward index creator for dictionary-encoded single and multi-value columns with forward index disabled.
 * This is a no-op.
 */
public class NoOpForwardIndexCreator implements ForwardIndexCreator {
  private final boolean _isSingleValue;

  public NoOpForwardIndexCreator(boolean isSingleValue) {
    _isSingleValue = isSingleValue;
  }

  @Override
  public boolean isDictionaryEncoded() {
    return true;
  }

  @Override
  public boolean isSingleValue() {
    return _isSingleValue;
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return FieldSpec.DataType.INT;
  }

  @Override
  public void putDictId(int dictId) {
  }

  @Override
  public void putDictIdMV(int[] dictIds) {
  }

  @Override
  public void close()
      throws IOException {
  }
}
