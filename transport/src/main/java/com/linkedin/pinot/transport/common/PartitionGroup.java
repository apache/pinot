package com.linkedin.pinot.transport.common;

import java.util.HashSet;
import java.util.Set;

/**
 * Partition Group abstraction. A partition group can be one or more partitions. Used to aggregate partitions
 * which have a common feature or to employ a function on them.
 */
public class PartitionGroup
{
  // Set of partitions that belong to this group
  private final Set<Partition> _partitions;

  public PartitionGroup()
  {
    _partitions = new HashSet<Partition>();
  }

  public void addPartition(Partition partition)
  {
    _partitions.add(partition);
  }

  public void removePartition(Partition partition)
  {
    _partitions.remove(partition);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (_partitions == null ? 0 : _partitions.hashCode());
    return result;
  }

  public Set<Partition> getPartitions() {
    return _partitions;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    PartitionGroup other = (PartitionGroup) obj;
    if (_partitions == null) {
      if (other._partitions != null)
        return false;
    } else if (!_partitions.equals(other._partitions))
      return false;
    return true;
  }
}