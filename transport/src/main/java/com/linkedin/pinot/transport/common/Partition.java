package com.linkedin.pinot.transport.common;

/**
 * Represent one partition in the system identified by a "long" type identifier
 * @author bvaradar
 *
 */
public class Partition {

  public static final Partition INVALID_PARTITION = new Partition(-1L);

  public final long _partitionNumer;

  /**
   * Construct a partition object with the given id.
   * @param id partition number
   */
  public Partition(long id)
  {
    _partitionNumer = id;
  }

  /**
   * Returns the partition id
   * @return partition id
   */
  public long getPartitionNumer() {
    return _partitionNumer;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (_partitionNumer ^ _partitionNumer >>> 32);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Partition other = (Partition) obj;
    if (_partitionNumer != other._partitionNumer)
      return false;
    return true;
  }
}
