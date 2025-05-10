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
package org.apache.pinot.spi.utils.hash;

import java.nio.ByteBuffer;

/**
 Note: This code is taken from
 <a href="https://github.com/tamtam180/CityHash-For-Java/blob/master/src/main/java/at/orz/hash/CityHash.java">...</a>
 and updated with the latest code in the original C++ impl: <a href="https://github.com/google/cityhash">...</a>
 */

public class CityHashFunctions {

    // Some primes between 2^63 and 2^64 for various uses.
    private static final long K0 = 0xc3a5c85c97cb3127L;
    private static final long K1 = 0xb492b66fbe98f273L;
    private static final long K2 = 0x9ae16a3b2f90404fL;

    private static final long K_MUL = 0x9ddfea08eb382d69L;

    // Magic numbers for 32-bit hashing.  Copied from Murmur3.
    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    private CityHashFunctions() {
    }

    /**
     * Computes 32-bit CityHash of input byte array from index pos to pos + len - 1
     */
    public static int cityHash32(byte[] s, int pos, int len) {
        if (len <= 24) {
            return len <= 12
                    ? (len <= 4
                    ? hash32Len0to4(s, pos, len) : hash32Len5to12(s, pos, len)) : hash32Len13to24(s, pos, len);
        }

        // len > 24
        int h = len;
        int g = C1 * h;
        int f = g;
        int a0 = rotate32(fetch32(s, pos + len - 4) * C1, 17) * C2;
        int a1 = rotate32(fetch32(s, pos + len - 8) * C1, 17) * C2;
        int a2 = rotate32(fetch32(s, pos + len - 16) * C1, 17) * C2;
        int a3 = rotate32(fetch32(s, pos + len - 12) * C1, 17) * C2;
        int a4 = rotate32(fetch32(s, pos + len - 20) * C1, 17) * C2;
        h ^= a0;
        h = rotate32(h, 19);
        h = h * 5 + 0xe6546b64;
        h ^= a2;
        h = rotate32(h, 19);
        h = h * 5 + 0xe6546b64;
        g ^= a1;
        g = rotate32(g, 19);
        g = g * 5 + 0xe6546b64;
        g ^= a3;
        g = rotate32(g, 19);
        g = g * 5 + 0xe6546b64;
        f += a4;
        f = rotate32(f, 19);
        f = f * 5 + 0xe6546b64;
        int iters = (len - 1) / 20;
        int currPos = pos;
        do {
            a0 = rotate32(fetch32(s, currPos) * C1, 17) * C2;
            a1 = fetch32(s, currPos + 4);
            a2 = rotate32(fetch32(s, currPos + 8) * C1, 17) * C2;
            a3 = rotate32(fetch32(s, currPos + 12) * C1, 17) * C2;
            a4 = fetch32(s, currPos + 16);
            h ^= a0;
            h = rotate32(h, 18);
            h = h * 5 + 0xe6546b64;
            f += a1;
            f = rotate32(f, 19);
            f = f * C1;
            g += a2;
            g = rotate32(g, 18);
            g = g * 5 + 0xe6546b64;
            h ^= a3 + a1;
            h = rotate32(h, 19);
            h = h * 5 + 0xe6546b64;
            g ^= a4;
            g = bswap32(g) * 5;
            h += a4 * 5;
            h = bswap32(h);
            f += a0;
            {
                //swap f and h
                int swap = f;
                f = h;
                h = swap;

                //swap f and g
                swap = f;
                f = g;
                g = swap;
            }
            currPos += 20;
        } while (--iters != 0);
        g = rotate32(g, 11) * C1;
        g = rotate32(g, 17) * C1;
        f = rotate32(f, 11) * C1;
        f = rotate32(f, 17) * C1;
        h = rotate32(h + g, 19);
        h = h * 5 + 0xe6546b64;
        h = rotate32(h, 17) * C1;
        h = rotate32(h + f, 19);
        h = h * 5 + 0xe6546b64;
        h = rotate32(h, 17) * C1;
        return h;
    }

    /**
     * Computes 32-bit CityHash of input byte array
     */
    public static long cityHash32(byte[] s) {
        return cityHash32(s, 0, s.length);
    }

    /**
     * Computes 64-bit CityHash of input byte array from index pos to pos + len - 1
     */
    public static long cityHash64(byte[] s, int pos, int len) {
        if (len <= 32) {
            if (len <= 16) {
                return hashLen0to16(s, pos, len);
            } else {
                return hashLen17to32(s, pos, len);
            }
        } else if (len <= 64) {
            return hashLen33to64(s, pos, len);
        }

        long x = fetch64(s, pos + len - 40);
        long y = fetch64(s, pos + len - 16) + fetch64(s, pos + len - 56);
        long z = hashLen16(fetch64(s, pos + len - 48) + len, fetch64(s, pos + len - 24));

        long [] v = weakHashLen32WithSeeds(s, pos + len - 64, len, z);
        long [] w = weakHashLen32WithSeeds(s, pos + len - 32, y + K1, x);
        x = x * K1 + fetch64(s, pos + 0);

        len = (len - 1) & (~63);
        do {
            x = rotate(x + y + v[0] + fetch64(s, pos + 8), 37) * K1;
            y = rotate(y + v[1] + fetch64(s, pos + 48), 42) * K1;
            x ^= w[1];
            y += v[0] + fetch64(s, pos + 40);
            z = rotate(z + w[0], 33) * K1;
            v = weakHashLen32WithSeeds(s, pos + 0, v[1] * K1, x + w[0]);
            w = weakHashLen32WithSeeds(s, pos + 32, z + w[1], y + fetch64(s, pos + 16));
            long swap = z;
            z = x;
            x = swap;
            pos += 64;
            len -= 64;
        } while (len != 0);

        return hashLen16(
                hashLen16(v[0], w[0]) + shiftMix(y) * K1 + z,
                hashLen16(v[1], w[1]) + x
        );
    }

    /**
     * Computes 64-bit CityHash of input byte array
     */
    public static long cityHash64(byte[] s) {
        return cityHash64(s, 0, s.length);
    }

    /**
     * Computes 64-bit CityHash of input byte array from index pos to pos + len - 1 and using a single seed value
     */
    public static long cityHash64WithSeed(byte[] s, int pos, int len, long seed) {
        return cityHash64WithSeeds(s, pos, len, K2, seed);
    }

    /**
     * Computes 64-bit CityHash of input byte array and using a single seed value
     */
    public static long cityHash64WithSeed(byte[] s, long seed) {
        return cityHash64WithSeed(s, 0, s.length, seed);
    }

    /**
     * Computes 64-bit CityHash of input byte array from index pos to pos + len - 1 and using two seed values
     */
    public static long cityHash64WithSeeds(byte[] s, int pos, int len, long seed0, long seed1) {
        return hashLen16(cityHash64(s, pos, len) - seed0, seed1);
    }

    /**
     * Computes 64-bit CityHash of input byte array and using two seed values
     */
    public static long cityHash64WithSeeds(byte[] s, long seed1, long seed2) {
        return cityHash64WithSeeds(s, 0, s.length, seed1, seed2);
    }

    /**
     * Computes 128-bit CityHash of input byte array from index pos to pos + len - 1  and using two seed values
     */
    public static long[] cityHash128WithSeed(byte[] s, int pos, int len, long seed0, long seed1) {
        if (len < 128) {
            return cityMurmur(s, pos, len, seed0, seed1);
        }

        long[] v = new long[2];
        long[] w = new long[2];
        long x = seed0;
        long y = seed1;
        long z = K1 * len;

        v[0] = rotate(y ^ K1, 49) * K1 + fetch64(s, pos);
        v[1] = rotate(v[0], 42) * K1 + fetch64(s, pos + 8);
        w[0] = rotate(y + z, 35) * K1 + x;
        w[1] = rotate(x + fetch64(s, pos + 88), 53) * K1;

        do {
            x = rotate(x + y + v[0] + fetch64(s, pos + 8), 37) * K1;
            y = rotate(y + v[1] + fetch64(s, pos + 48), 42) * K1;

            x ^= w[1];
            y += v[0] + fetch64(s, pos + 40);
            z = rotate(z + w[0], 33) * K1;
            v = weakHashLen32WithSeeds(s, pos + 0, v[1] * K1, x + w[0]);
            w = weakHashLen32WithSeeds(s, pos + 32, z + w[1], y + fetch64(s, pos + 16));
            {
                long swap = z;
                z = x;
                x = swap;
            }
            pos += 64;
            x = rotate(x + y + v[0] + fetch64(s, pos + 8), 37) * K1;
            y = rotate(y + v[1] + fetch64(s, pos + 48), 42) * K1;
            x ^= w[1];
            y += v[0] + fetch64(s, pos + 40);
            z = rotate(z + w[0], 33) * K1;
            v = weakHashLen32WithSeeds(s, pos, v[1] * K1, x + w[0]);
            w = weakHashLen32WithSeeds(s, pos + 32, z + w[1], y + fetch64(s, pos + 16));
            {
                long swap = z;
                z = x;
                x = swap;
            }
            pos += 64;
            len -= 128;
        } while (len >= 128);

        x += rotate(v[0] + z, 49) * K0;
        y = y * K0 + rotate(w[1], 37);
        z = z * K0 + rotate(w[0], 27);
        w[0] *= 9;
        v[0] *= K0;

        for (int tailDone = 0; tailDone < len; ) {
            tailDone += 32;
            y = rotate(x + y, 42) * K0 + v[1];
            w[0] += fetch64(s, pos + len - tailDone + 16);
            x = x * K0 + w[0];
            z += w[1] + fetch64(s, pos + len - tailDone);
            w[1] += v[0];
            v = weakHashLen32WithSeeds(s, pos + len - tailDone, v[0] + z, v[1]);
            v[0] *= K0;
        }

        x = hashLen16(x, v[0]);
        y = hashLen16(y + z, w[0]);

        return new long[]{
                hashLen16(x + v[1], w[1]) + y,
                hashLen16(x + w[1], y + v[1])
        };
    }
    /**
     * Computes 128-bit CityHash of input byte array from index pos to pos + len - 1
     */
    public static long[] cityHash128(byte[] s, int pos, int len) {
        if (len >= 16) {
            return cityHash128WithSeed(
                    s, pos + 16,
                    len - 16,
                    fetch64(s, pos),
                    fetch64(s, pos + 8) + K0);
        } else {
            return cityHash128WithSeed(s, pos, len, K0, K1);
        }
    }

    /**
     * Computes 128-bit CityHash of input byte array
     */
    public static byte[] cityHash128(byte[] s) {
        long[] hash = cityHash128(s, 0, s.length);
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(hash[0]);
        buffer.putLong(hash[1]);
        return buffer.array();
    }

    private static long[] cityMurmur(byte[] s, int pos, int len, long seed0, long seed1) {
        long a = seed0;
        long b = seed1;
        long c = 0;
        long d = 0;

        if (len <= 16) {
            a = shiftMix(a * K1) * K1;
            c = b * K1 + hashLen0to16(s, pos, len);
            d = shiftMix(a + (len >= 8 ? fetch64(s, pos) : c));
        } else {
            c = hashLen16(fetch64(s, pos + len - 8) + K1, a);
            d = hashLen16(b + len, c + fetch64(s, pos + len - 16));
            a += d;

            do {
                a ^= shiftMix(fetch64(s, pos + 0) * K1) * K1;
                a *= K1;
                b ^= a;
                c ^= shiftMix(fetch64(s, pos + 8) * K1) * K1;
                c *= K1;
                d ^= c;
                pos += 16;
                len -= 16;
            } while (len > 16);
        }

        a = hashLen16(a, c);
        b = hashLen16(d, b);

        return new long[]{ a ^ b, hashLen16(b, a) };
    }

    private static long toLongLE(byte[] b, int i) {
        return (((long) b[i + 7] << 56)
                + ((long) (b[i + 6] & 255) << 48)
                + ((long) (b[i + 5] & 255) << 40)
                + ((long) (b[i + 4] & 255) << 32)
                + ((long) (b[i + 3] & 255) << 24)
                + ((b[i + 2] & 255) << 16)
                + ((b[i + 1] & 255) << 8)
                + ((b[i + 0] & 255) << 0));
    }

    private static int toIntLE(byte[] b, int i) {
        return (((b[i + 3] & 255) << 24) + ((b[i + 2] & 255) << 16) + ((b[i + 1] & 255) << 8)
                + ((b[i + 0] & 255) << 0));
    }

    private static long shiftMix(long val) {
        return val ^ (val >>> 47);
    }

    private static long fetch64(byte[] s, int pos) {
        return toLongLE(s, pos);
    }

    private static int fetch32(byte[] s, int pos) {
        return toIntLE(s, pos);
    }

    private static long rotate(long val, int shift) {
        return shift == 0 ? val : (val >>> shift) | (val << (64 - shift));
    }

    private static int rotate32(int val, int shift) {
        // Avoid shifting by 32: doing so yields an undefined result.
        return shift == 0 ? val : ((val >>> shift) | (val << (32 - shift)));
    }

    private static long hash128to64(long u, long v) {
        long a = (u ^ v) * K_MUL;
        a ^= (a >>> 47);
        long b = (v ^ a) * K_MUL;
        b ^= (b >>> 47);
        b *= K_MUL;
        return b;
    }

    private static long hashLen16(long u, long v) {
        return hash128to64(u, v);
    }

    private static long hashLen16(long u, long v, long mul) {
        // Murmur-inspired hashing.
        long a = (u ^ v) * mul;
        a ^= (a >>> 47);
        long b = (v ^ a) * mul;
        b ^= (b >>> 47);
        b *= mul;
        return b;
    }

    private static long hashLen0to16(byte[] s, int pos, int len) {
        if (len >= 8) {
            long mul = K2 + (len * 2L);
            long a = fetch64(s, pos) + K2;
            long b = fetch64(s, pos + len - 8);
            long c = (rotate(b, 37) * mul) + a;
            long d = (rotate(a, 25) + b) * mul;
            return hashLen16(c, d, mul);
        }
        if (len >= 4) {
            long mul = K2 + (len * 2);
            long a = 0xffffffffL & fetch32(s, pos);
            return hashLen16((a << 3) + len, 0xffffffffL & fetch32(s, pos + len - 4), mul);
        }
        if (len > 0) {
            int a = s[pos] & 0xff;
            int b = s[pos + (len >>> 1)] & 0xff;
            int c = s[pos + len - 1] & 0xff;
            int y = a + (b << 8);
            int z = len + (c << 2);
            return shiftMix(y * K2 ^ z * K0) * K2;
        }
        return K2;
    }

    private static long hashLen17to32(byte[] s, int pos, int len) {
        long mul = K2 + (len * 2L);
        long a = fetch64(s, pos) * K1;
        long b = fetch64(s, pos + 8);
        long c = fetch64(s, pos + len - 8) * mul;
        long d = fetch64(s, pos + len - 16) * K2;
        return hashLen16(rotate(a + b, 43) + rotate(c, 30) + d,
                a + rotate(b + K2, 18) + c, mul);
    }

    private static long[] weakHashLen32WithSeeds(long w, long x, long y, long z, long a, long b) {
        a += w;
        b = rotate(b + a + z, 21);
        long c = a;
        a += x;
        a += y;
        b += rotate(a, 44);
        return new long[]{ a + z, b + c };
    }

    private static long[] weakHashLen32WithSeeds(byte[] s, int pos, long a, long b) {
        return weakHashLen32WithSeeds(
                fetch64(s, pos + 0),
                fetch64(s, pos + 8),
                fetch64(s, pos + 16),
                fetch64(s, pos + 24),
                a,
                b
        );
    }

    private static long hashLen33to64(byte[] s, int pos, int len) {
        long mul = K2 + len * 2;
        long a = fetch64(s, pos) * K2;
        long b = fetch64(s, pos + 8);
        long c = fetch64(s, pos + len - 24);
        long d = fetch64(s, pos + len - 32);
        long e = fetch64(s, pos + 16) * K2;
        long f = fetch64(s, pos + 24) * 9;
        long g = fetch64(s, pos + len - 8);
        long h = fetch64(s, pos + len - 16) * mul;
        long u = rotate(a + g, 43) + (rotate(b, 30) + c) * 9;
        long v = ((a + g) ^ d) + f + 1;
        long w = bswap64((u + v) * mul) + h;
        long x = rotate(e + f, 42) + c;
        long y = (bswap64((v + w) * mul) + g) * mul;
        long z = e + f + c;
        a = bswap64((x + z) * mul + y) + b;
        b = shiftMix((z + a) * mul + d + h) * mul;

        return b + x;
    }

    // A 32-bit to 32-bit integer hash copied from Murmur3.
    private static int fmix(int h) {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }

    // Helper from Murmur3 for combining two 32-bit values.
    private static int mur(int a, int h) {
        a *= C1;
        a = rotate32(a, 17);
        a *= C2;
        h ^= a;
        h = rotate32(h, 19);
        return (h * 5) + 0xe6546b64;
    }

    private static int hash32Len0to4(byte[] s, int pos, int len) {
        int b = 0;
        int c = 9;
        for (int i = 0; i < len; i++) {
            int v = s[pos + i];
            b = (b * C1) + v;
            c ^= b;
        }
        return fmix(mur(b, mur(len, c)));
    }

    private static int hash32Len5to12(byte[] s, int pos, int len) {
        int a = len;
        int b = a * 5;
        int c = 9;
        int d = b;
        a += fetch32(s, pos);
        b += fetch32(s, pos + len - 4);
        c += fetch32(s, (((pos + len) >> 1) & 4));
        return fmix(mur(c, mur(b, mur(a, d))));
    }

    private static int hash32Len13to24(byte[] s, int pos, int len) {
        int a = fetch32(s, pos - 4 + (len >> 1));
        int b = fetch32(s, pos + 4);
        int c = fetch32(s, pos + len - 8);
        int d = fetch32(s, pos + (len >> 1));
        int e = fetch32(s, pos);
        int f = fetch32(s, pos + len - 4);
        int h = len;

        return fmix(mur(f, mur(e, mur(d, mur(c, mur(b, mur(a, h)))))));
    }

    private static int bswap32(int x) {
        return ((((x) & 0xff000000) >>> 24) | (((x) & 0x00ff0000) >>> 8)
                | (((x) & 0x0000ff00) << 8) | (((x) & 0x000000ff) << 24));
    }

    private static long bswap64(long x) {
        return ((((x) & 0xff00000000000000L) >>> 56)
                | (((x) & 0x00ff000000000000L) >>> 40)
                | (((x) & 0x0000ff0000000000L) >>> 24)
                | (((x) & 0x000000ff00000000L) >>> 8)
                | (((x) & 0x00000000ff000000L) << 8)
                | (((x) & 0x0000000000ff0000L) << 24)
                | (((x) & 0x000000000000ff00L) << 40)
                | (((x) & 0x00000000000000ffL) << 56));
    }
}
