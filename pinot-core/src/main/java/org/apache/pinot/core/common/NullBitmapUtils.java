package org.apache.pinot.core.common;

import java.io.DataOutputStream;
import java.io.IOException;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public class NullBitmapUtils {
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
