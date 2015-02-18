package com.linkedin.pinot.common.utils;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;


public class HashUtil {
  public static final byte[] EMPTY_BYTES = new byte[0];
  public static final String NULL_STRING = "NULL";
  public static final String MULTI_VALUE_HEAD_STRING = "MULTI:";

  // Given a row of doc and data schema, we only compute hash code on dimension columns.
  // Tried many ways to serialize Object[] to byte[]. Based on my tests, concatenating those objects
  // as a string is the fastest way. About 80%+ speed up than java.io.ObjectOutputStream.

  public static long compute(IntBuffer buff) {
    buff.rewind();
    int[] inBytes = buff.array();
    ByteBuffer bBuff = ByteBuffer.allocate(buff.array().length * 4);
    for (int i : buff.array())
      bBuff.putInt(i);

    return hash64(bBuff.array(), inBytes.length);
  }

  public static long hash64(final byte[] data, int length) {
    // Default seed is 0xe17a1465.
    return hash64(data, length, 0xe17a1465);
  }

  // Implement 64-bit Murmur2 hash.
  public static long hash64(final byte[] data, int length, int seed) {
    final long m = 0xc6a4a7935bd1e995L;
    final int r = 47;

    long h = (seed & 0xffffffffl) ^ (length * m);

    int length8 = length / 8;

    for (int i = 0; i < length8; i++) {
      final int i8 = i * 8;
      long k =
          ((long) data[i8 + 0] & 0xff) + (((long) data[i8 + 1] & 0xff) << 8) + (((long) data[i8 + 2] & 0xff) << 16)
              + (((long) data[i8 + 3] & 0xff) << 24) + (((long) data[i8 + 4] & 0xff) << 32)
              + (((long) data[i8 + 5] & 0xff) << 40) + (((long) data[i8 + 6] & 0xff) << 48)
              + (((long) data[i8 + 7] & 0xff) << 56);

      k *= m;
      k ^= k >>> r;
      k *= m;

      h ^= k;
      h *= m;
    }

    switch (length % 8) {
      case 7:
        h ^= (long) (data[(length & ~7) + 6] & 0xff) << 48;
      case 6:
        h ^= (long) (data[(length & ~7) + 5] & 0xff) << 40;
      case 5:
        h ^= (long) (data[(length & ~7) + 4] & 0xff) << 32;
      case 4:
        h ^= (long) (data[(length & ~7) + 3] & 0xff) << 24;
      case 3:
        h ^= (long) (data[(length & ~7) + 2] & 0xff) << 16;
      case 2:
        h ^= (long) (data[(length & ~7) + 1] & 0xff) << 8;
      case 1:
        h ^= (long) (data[length & ~7] & 0xff);
        h *= m;
    }
    ;

    h ^= h >>> r;
    h *= m;
    h ^= h >>> r;
    return h;
  }

}
