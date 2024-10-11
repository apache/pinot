package org.apache.pinot.calcite.rel.rules;

import javax.annotation.Nullable;
import org.immutables.value.Value;


/**
 * An internal interface used to generate the table options hint.
 */
@Value.Immutable
public interface TableOptions {
  String getPartitionKey();

  String getPartitionFunction();

  int getPartitionSize();

  @Nullable
  Integer getPartitionParallelism();
}
