package com.linkedin.thirdeye.anomaly.driver;

import java.math.BigInteger;
import java.util.List;
import java.util.Random;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;


/**
 *
 */
public class DimensionKeySeriesPartitioner {

  private final int partitions;

  private int seed = 0;
  private boolean useHash = false;
  private List<DimensionKeySeries> resource;

  public DimensionKeySeriesPartitioner(int partitions) {
    super();
    this.partitions = partitions;
  }

  /**
   * If non-zero, then the partition mappings will be shuffled with this random seed.
   */
  public DimensionKeySeriesPartitioner setSeed(int seed) {
    this.seed = seed;
    return this;
  }

  /**
   * Use the hash based scheme as opposed to the default round-robin.
   */
  public DimensionKeySeriesPartitioner useHash() {
    this.useHash = true;
    return this;
  }

  /**
   * The list of dimensionKeySeries to partition.
   */
  public DimensionKeySeriesPartitioner setResource(List<DimensionKeySeries> resource) {
    this.resource = resource;
    return this;
  }

  /**
   * @return
   *  Resource partitions.
   */
  public Multimap<Integer, DimensionKeySeries> partition() {
    int[] partitionMapping = getPartitionMapping();

    Multimap<Integer, DimensionKeySeries> resourcePartitions = ArrayListMultimap.create();
    if (useHash) {
      // partition based on dimensionKey's md5
      for (DimensionKeySeries d : resource) {
        BigInteger dimensionKeyHash = new BigInteger(d.getDimensionKey().toMD5());
        int partitionIndex = dimensionKeyHash.mod(BigInteger.valueOf(partitions)).intValue();
        resourcePartitions.put(partitionMapping[partitionIndex], d);
      }
    } else {
      // partition by round robin
      int currPartition = 0;
      for (DimensionKeySeries d : resource) {
        resourcePartitions.put(partitionMapping[currPartition % partitions], d);
        currPartition++;
      }
    }
    return resourcePartitions;
  }

  private int[] getPartitionMapping() {
    int[] arr = new int[partitions];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = i;
    }

    // shuffle the mapping
    if (seed != 0) {
      Random rand = new Random(seed);
      for (int i = 0; i < arr.length; i++) {
        int j = rand.nextInt(arr.length);
        int tmp = arr[j]; // swap
        arr[j] = arr[i];
        arr[i] = tmp;
      }
    }

    return arr;
  }

}
