package com.linkedin.thirdeye.dataframe;

class JoinHashMap {
  private static final int M = 0x5bd1e995;

  private static final int MAX_VALUE = 0x7FFFFFFE;
  private static final int VALUE_MASK = 0x7FFFFFFF;
  private static final long NEXT_FLAG = 0x0000000080000000L;

  private static final double SCALING_FACTOR = 2;

  private static final int SEED = 0xAE928F20;

  private final int shift;
  private final long[] data;

  private long collisions = 0;
  private long rereads = 0;

  // 0xHHHHHHHH[fv]VVVVVVV
  //
  // H hash
  // F has next flag
  // V value (+1)
  //
  // 0x0000000000000000    indicates empty
  // 0x........[1.]....... indicates next element exists

  public JoinHashMap(Series... series) {
    Series.assertSameLength(series);

    this.shift = log2(series[0].size()) + 1;
    this.data = new long[pow2(this.shift)];

    if(series[0].size() > MAX_VALUE)
      throw new IllegalArgumentException(String.format("Cannot store store values greater than %d", MAX_VALUE));

    for(int i=0; i<series[0].size(); i++) {
      put(hashRow(series, i), i);
    }
  }

  public JoinHashMap(int minSize) {
    int size = (int)(minSize * SCALING_FACTOR);
    this.shift = log2(size) + 1;
    this.data = new long[pow2(this.shift)];
  }

  public void put(int key, int value) {
    if(value > MAX_VALUE || (value & NEXT_FLAG) != 0)
      throw new IllegalArgumentException(String.format("Value must be between 0 and %d", MAX_VALUE));
    int step = 1;
    int hash = hash(key);
    long tuple = tuple(key, value + 1); // ensure 0 indicates empty
    long ttup = fetch(hash);
    while(ttup != 0) {
//      this.data[hash2index(hash)] = ttup | NEXT_FLAG;
      hash += step * step++;
      ttup = fetch(hash);
      this.collisions++;
    }
    this.data[hash2index(hash)] = tuple;
  }

  public int get(int key, int offset) {
//    int steps = 0;
    int step = 1;
    int toff = 0;
    int hash = hash(key);
    long tuple = fetch(hash);
    while(tuple != 0) {
      int tkey = tuple2key(tuple);
      int tval = tuple2val(tuple);

      if(tkey == key) {
        if(offset == toff++)
          return (tval & VALUE_MASK) - 1; // fix value offset
      }
//      if(!tupleHasNext(tuple))
//        return -1;

      hash += step * step++;
      tuple = fetch(hash);
      this.rereads++;

//      // NOTE: this could enter a cycle, catch it
//      if(this.data.length <= steps++)
//        throw new IllegalArgumentException("Cycle detected");
    }
    return -1;
  }

  private long fetch(int hash) {
    return this.data[hash2index(hash)];
  }

//  public int get(Series[] series, int rowIndex, int offset) {
//    int hash = hashCode(series, rowIndex);
//    long tuple = this.data[hash2index(hash)];
//    if(tuple == 0)
//      return -1;
//
//    while(tuple2hash(tuple) != hash) {
//      hash = nextHash(hash);
//      tuple = this.data[hash2index(hash)];
//      if(tuple == 0)
//        return -1;
//    }
//
//    for(int i=0; i<offset; i++) {
//      hash = nextHash(hash);
//    }
//
//    return tuple2val(this.data[hash2index(hash)]) - 1;
//  }

  public int size() {
    return this.data.length;
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

  static boolean tupleHasNext(long tuple) {
    return (((int)tuple) & NEXT_FLAG) != 0;
  }

  static long tuple(int key, int val) {
    return (((long)key) << 32) | (long)val;
  }

//  static int hashCode(Series[] series, int rowIndex) {
//    // TODO efficient hashing
//    int value = 0;
//    for(int i=0; i<series.length; i++) {
//      value = M * value + series[i].hashCode(rowIndex);
//      value ^= value >>> 13;
//    }
//    return value;
//  }

  static int hashRow(Series[] series, int row) {
    return 0;
  }

//  static int hash(int hash) {
//    hash += M;
//    hash *= M;
//    hash ^= hash >>> 13;
//    return hash;
//  }
//
  static int log2(int value) {
    if(value == 0)
      return 0;
    return 31 - Integer.numberOfLeadingZeros(value);
  }

  static int pow2(int value) {
    return 1 << value;
  }

  static int hash(int k) {
    final int m = 0x5bd1e995;
    final int r = 24;
    k *= m;
    k ^= k >>> r;
    return k;
  }

//  // murmur hash, derived from apache hadoop
//  static int hash(int value) {
//    final int m = 0x5bd1e995;
//    final int r = 24;
//
//    int h = SEED;
//    int k = value;
//
//    k *= m;
//    k ^= k >>> r;
//    k *= m;
//
//    h *= m;
//    h ^= k;
//
//    h ^= h >>> 13;
//    h *= m;
//    h ^= h >>> 15;
//
//    return h;
//  }

  public static void main(String[] args) {
    JoinHashMap m = new JoinHashMap(100);
    for(int i=0; i<100; i++) {
      m.put(i, i);
    }
    System.out.println(m.visualize());
  }

}
