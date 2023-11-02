package org.apache.pinot.segment.spi.datasource;

import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * This enum specify the null semantics that should be used, usually in the context of a query.
 */
public enum NullMode {
  /**
   * In this mode columns are considered not nullable. Null vectors stored in the segments are ignored and V1 query
   * engine does not expect to see null values in the blocks.
   */
  NONE_NULLABLE,
  /**
   * In this mode all columns are considerable nullable (even if {@link FieldSpec#getNullable()}) is false.
   *
   * Columns will have a null vector if and only if the segment contains a null vector for the column.
   * This is not always true, given that depends on the value of {@link IndexingConfig#isNullHandlingEnabled()} at the
   * time the segment was created.
   *
   * In this mode, V1 engine must expect to receive null blocks.
   */
  ALL_NULLABLE,
  /**
   * In this mode a column is considered nullable if and only if {@link FieldSpec#getNullable()}) is true.
   *
   * Columns will have a null vector if and only if the column is nullable and the segment contains a null vector for
   * the column.
   * This is not always true, given that depends on the value of {@link IndexingConfig#isNullHandlingEnabled()} at the
   * time the segment was created.
   *
   * In this mode, V1 engine must expect to receive null blocks.
   */
  COLUMN_BASED;

  /**
   * Returns true if and only if V1 query engine must expect nulls in their values.
   */
  public boolean nullAtQueryTime() {
    return this != NullMode.NONE_NULLABLE;
  }
}
