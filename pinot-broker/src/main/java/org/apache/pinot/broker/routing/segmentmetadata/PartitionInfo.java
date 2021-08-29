package org.apache.pinot.broker.routing.segmentmetadata;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;

public class PartitionInfo {
  public static final PartitionInfo INVALID_PARTITION_INFO = new PartitionInfo(null, null);
  public final PartitionFunction _partitionFunction;
  public final Set<Integer> _partitions;
  private final int _hashCode;

  public PartitionInfo(PartitionFunction partitionFunction, Set<Integer> partitions) {
    _partitionFunction = partitionFunction;
    _partitions = partitions;
    _hashCode = Objects.hash(_partitionFunction, _partitions);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (hashCode() != o.hashCode()) {
      return false;
    }
    PartitionInfo that = (PartitionInfo) o;
    return Objects.equals(_partitions, that._partitions) && Objects.equals(_partitionFunction, that._partitionFunction);
  }

  @Override
  public int hashCode() {
    return _hashCode;
  }

  @Nullable
  public static Set<String> getPartitionColumnFromConfig(TableConfig tableConfig) {
    SegmentPartitionConfig segmentPartitionConfig = tableConfig.getIndexingConfig().getSegmentPartitionConfig();
    if (segmentPartitionConfig == null) {
      return null;
    }
    Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
    if (columnPartitionMap == null) {
      return Collections.emptySet();
    }
    return Collections.unmodifiableSet(columnPartitionMap.keySet());
  }
}
