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
package org.apache.pinot.core.io.util;

import org.apache.pinot.core.segment.memory.PinotDataBuffer;

public class PinotDataBitSetFactory {

  public static PinotBitSet createBitSet(PinotDataBuffer pinotDataBuffer, int numBitsPerValue) {
    switch (numBitsPerValue) {
      // for power of 2 encodings, use the faster vectorized reader
      case 1:
        return new PinotDataBitSetV2.Bit1Encoded(pinotDataBuffer, numBitsPerValue);
      case 2:
        return new PinotDataBitSetV2.Bit2Encoded(pinotDataBuffer, numBitsPerValue);
      case 4:
        return new PinotDataBitSetV2.Bit4Encoded(pinotDataBuffer, numBitsPerValue);
      case 8:
        return new PinotDataBitSetV2.Bit8Encoded(pinotDataBuffer, numBitsPerValue);
      case 16:
        return new PinotDataBitSetV2.Bit16Encoded(pinotDataBuffer, numBitsPerValue);
      case 32:
        return new PinotDataBitSetV2.RawInt(pinotDataBuffer, numBitsPerValue);
      // for non-power of 2 encodings, fallback to the older reader
      default:
        return new PinotDataBitSet(pinotDataBuffer, numBitsPerValue);
    }
  }
}
