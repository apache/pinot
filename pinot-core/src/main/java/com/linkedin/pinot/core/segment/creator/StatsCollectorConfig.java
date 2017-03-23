/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.creator;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.partition.PartitionFunction;
import com.linkedin.pinot.core.indexsegment.generator.SegmentPartitionConfig;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang.math.IntRange;


/**
 * Config class for Stats collector, contains all the configs and parameters required to build
 * the stats collector.
 */
public class StatsCollectorConfig {

  private final Schema _schema;
  private final SegmentPartitionConfig _segmentPartitionConfig;

  /**
   * Constructor for the class.
   * @param schema Data schema
   * @param segmentPartitionConfig Segment partitioning config
   */
  public StatsCollectorConfig(@Nonnull Schema schema, SegmentPartitionConfig segmentPartitionConfig) {
    Preconditions.checkNotNull(schema);
    _schema = schema;
    _segmentPartitionConfig = segmentPartitionConfig;
  }

  @Nullable
  public FieldSpec getFieldSpecForColumn(String column) {
    return _schema.getFieldSpecFor(column);
  }

  @Nullable
  public PartitionFunction getPartitionFunction(String column) {
    return (_segmentPartitionConfig != null) ? _segmentPartitionConfig.getPartitionFunction(column) : null;
  }

  @Nullable
  public List<IntRange> getPartitionValue(String column) {
    return (_segmentPartitionConfig != null) ? _segmentPartitionConfig.getPartitionRanges(column) : null;
  }

  @Nonnull
  public Schema getSchema() {
    return _schema;
  }
}
