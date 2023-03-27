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
package org.apache.pinot.foreign.memory;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import javax.annotation.Nullable;


public class LittleEndianForeignMemoryPinotDataBuffer extends AbstractForeignMemoryPinotDataBuffer {

  public static final ValueLayout.OfChar OF_CHAR = ValueLayout.JAVA_CHAR_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  public static final ValueLayout.OfShort OF_SHORT =
      ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  public static final ValueLayout.OfInt OF_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  public static final ValueLayout.OfFloat OF_FLOAT =
      ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  public static final ValueLayout.OfLong OF_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  public static final ValueLayout.OfDouble OF_DOUBLE =
      ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  public LittleEndianForeignMemoryPinotDataBuffer(MemorySegment memorySegment, @Nullable Arena arena) {
    super(memorySegment, arena);
  }

  @Override
  protected ValueLayout.OfChar getOfChar() {
    return OF_CHAR;
  }

  @Override
  protected ValueLayout.OfShort getOfShort() {
    return OF_SHORT;
  }

  @Override
  protected ValueLayout.OfInt getOfInt() {
    return OF_INT;
  }

  @Override
  protected ValueLayout.OfFloat getOfFloat() {
    return OF_FLOAT;
  }

  @Override
  protected ValueLayout.OfLong getOfLong() {
    return OF_LONG;
  }

  @Override
  protected ValueLayout.OfDouble getOfDouble() {
    return OF_DOUBLE;
  }

  @Override
  public ByteOrder order() {
    return ByteOrder.LITTLE_ENDIAN;
  }
}
