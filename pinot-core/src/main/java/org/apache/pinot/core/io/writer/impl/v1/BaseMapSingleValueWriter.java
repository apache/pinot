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
package org.apache.pinot.core.io.writer.impl.v1;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Map;
import org.apache.pinot.core.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.core.io.writer.MapSingleValueWriter;
import org.apache.pinot.core.io.writer.impl.FixedByteSingleValueMultiColWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;

/**
 * Writer to be used in Batch Mode where keys and values are dictionary encoded.
 */
public abstract class BaseMapSingleValueWriter implements MapSingleValueWriter {

  @Override
  public void setIntIntMap(int row, Map<Integer, Integer> map) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setStringIntMap(int row, Map<String, Integer> map) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setStringLongMap(int row, Map<Integer, Long> map) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setStringFloatMap(int row, Map<Integer, Float> map) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setStringDoubleMap(int row, Map<Integer, Double> map) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setStringStringMap(int row, Map<String, String> map) {
    throw new UnsupportedOperationException();
  }
}
