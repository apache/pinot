package com.linkedin.thirdeye.dataframe;

import java.util.Arrays;


class JoinHashMap {
  private static final int M = 0x5bd1e995;
  private static final int SEED = 0xb7f93ea;
  private static final long TO_LONG = 0xFFFFFFFFL;
  private static final int INITIAL_SIZE = 1;

  public static final int RESERVED_VALUE = 0xFFFFFFFF;

  private static final double SCALING_FACTOR = 2;

  private final int maxSize;
  private final int shift;
  private final long[] data;

  private int size;
  private long collisions = 0;
  private long rereads = 0;

  private int iteratorKey = -1;
  private int iterator = -1;
  private int[] outBuffer = new int[INITIAL_SIZE];

  // 0xHHHHHHHHVVVVVVVV
  //
  // H hash
  // V value (+1)
  //
  // 0x0000000000000000    indicates empty

  public JoinHashMap(Series... series) {
    this(series[0].size());
    Series.assertSameLength(series);

    for(int i=0; i<series[0].size(); i++) {
      put(hashRow(series, i), i);
    }
  }

  public int[] get(Series[] series, int row) {
    int key = hashRow(series, row);
    int val = this.get(key);

    int cntr = 0;
    while(val != -1) {
      if(cntr >= this.outBuffer.length)
        this.outBuffer = new int[this.outBuffer.length * 2];
      this.outBuffer[cntr++] = val;
      val = this.getNext();
    }
    return Arrays.copyOf(this.outBuffer, cntr);
  }

  static int hashRow(Series[] series, int row) {
    int k = SEED;
    for(Series s : series) {
      k = hash(s.hashCode(row) ^ k);
    }
    return k;
  }

  public JoinHashMap(int maxSize) {
    this.maxSize = maxSize;
    int minCapacity = (int)(maxSize * SCALING_FACTOR);
    this.shift = log2(minCapacity) + 1;
    this.data = new long[pow2(this.shift)];
  }

  public void put(int key, int value) {
    // NOTE: conservative - (hash, value) must be != 0
    if(value == RESERVED_VALUE)
      throw new IllegalArgumentException(String.format("Value must be different from %d", RESERVED_VALUE));
    if(this.size >= this.maxSize)
      throw new IllegalArgumentException(String.format("Map is at max size %d", this.maxSize));
    int hash = hash(key);
    long tuple = tuple(key, value + 1); // ensure 0 indicates empty
    long ttup = fetch(hash);
    while(ttup != 0) {
      hash += 1;
      ttup = fetch(hash);
      this.collisions++;
    }
    this.data[hash2index(hash)] = tuple;
    this.size++;
  }

  public int get(int key) {
    return get(key, 0);
  }

  public int get(int key, int offset) {
    return getInternal(key, hash(key), offset);
  }

  private int getInternal(int key, int hash, int offset) {
    int toff = 0;
    long tuple = fetch(hash);
    while(tuple != 0) {
      int tkey = tuple2key(tuple);

      if(tkey == key) {
        if(offset == toff++) {
          this.iteratorKey = key;
          this.iterator = hash + 1;
          return tuple2val(tuple) - 1; // fix value offset
        }
      }

      hash += 1;
      tuple = fetch(hash);
      this.rereads++;
    }

    this.iteratorKey = 0;
    this.iterator = -1;
    return -1;
  }

  public int getNext() {
    if(this.iterator == -1)
      return -1;
    return getInternal(this.iteratorKey, this.iterator, 0);
  }

  private long fetch(int hash) {
    return this.data[hash2index(hash)];
  }

  public int size() {
    return this.data.length;
  }

  public int getMaxSize() {
    return this.maxSize;
  }

  public long getCollisions() {
    return this.collisions;
  }

  public long getRereads() {
    return rereads;
  }

  public String visualize() {
    final int rowSize = (int) (Math.ceil(Math.sqrt(this.data.length)) * 1.5);
    final StringBuilder sb = new StringBuilder();
    for(int i=0; i<this.size(); i++) {
      if(i % rowSize == 0)
        sb.append('\n');
      if(this.data[i] == 0)
        sb.append('.');
      else
        sb.append('X');
    }
    return sb.toString();
  }

  int hash2index(int hash) {
    return hash & ((1 << this.shift) - 1);
  }

  static int tuple2key(long tuple) {
    return (int) (tuple >>> 32);
  }

  static int tuple2val(long tuple) {
    return (int) tuple;
  }

  static long tuple(int key, int val) {
    return ((key & TO_LONG) << 32) | (val & TO_LONG);
  }

  static int log2(int value) {
    if(value == 0)
      return 0;
    return 31 - Integer.numberOfLeadingZeros(value);
  }

  static int pow2(int value) {
    return 1 << value;
  }

  static int hash(int k) {
    final int r = 24;
    k *= M;
    k ^= k >>> r;
    return k;
  }

  public static void main(String[] args) {
    JoinHashMap m = new JoinHashMap(100);
    for(int i=0; i<100; i++) {
      m.put(i, i);
    }
    System.out.println(m.visualize());
  }

}
