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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.roaringbitmap.RoaringBitmap;


public final class NullBitmapUtils {
  private NullBitmapUtils() {
  }

  public static void setNullRowIds(RoaringBitmap nullBitmap, ByteArrayOutputStream fixedSizeByteArrayOutputStream,
      ByteArrayOutputStream variableSizeDataByteArrayOutputStream)
      throws IOException {
    if (nullBitmap != null) {
      writeInt(fixedSizeByteArrayOutputStream, variableSizeDataByteArrayOutputStream.size());
      if (nullBitmap.isEmpty()) {
        writeInt(fixedSizeByteArrayOutputStream, 0);
      } else {
        byte[] nullBitmapBytes = ObjectSerDeUtils.ROARING_BITMAP_SER_DE.serialize(nullBitmap);
        writeInt(fixedSizeByteArrayOutputStream, nullBitmapBytes.length);
        variableSizeDataByteArrayOutputStream.write(nullBitmapBytes);
      }
    }
  }

  private static void writeInt(ByteArrayOutputStream out, int value) {
    out.write((value >>> 24) & 0xFF);
    out.write((value >>> 16) & 0xFF);
    out.write((value >>> 8) & 0xFF);
    out.write(value & 0xFF);
  }
}
