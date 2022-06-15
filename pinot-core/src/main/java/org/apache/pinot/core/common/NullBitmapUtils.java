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
package org.apache.pinot.core.common;

import java.io.DataOutputStream;
import java.io.IOException;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public final class NullBitmapUtils {
  private NullBitmapUtils() {
  }

  public static void setNullRowIds(ImmutableRoaringBitmap nullBitmap, DataOutputStream fixedSizeNullVectorOutputStream,
      DataOutputStream variableSizeNullVectorOutputStream)
      throws IOException {
    fixedSizeNullVectorOutputStream.writeInt(variableSizeNullVectorOutputStream.size());
    if (nullBitmap.isEmpty()) {
      fixedSizeNullVectorOutputStream.writeInt(0);
    } else {
      int[] nullBitmapArray = nullBitmap.toArray();
      fixedSizeNullVectorOutputStream.writeInt(nullBitmapArray.length);
      // todo: optimize looping through bitmaps.
      for (int nullBitmapInt : nullBitmapArray) {
        variableSizeNullVectorOutputStream.writeInt(nullBitmapInt);
      }
    }
  }
}
