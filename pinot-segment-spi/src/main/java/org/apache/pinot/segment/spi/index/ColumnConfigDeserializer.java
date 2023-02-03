package org.apache.pinot.segment.spi.index;

import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


@FunctionalInterface
public interface ColumnConfigDeserializer<C> {

  /**
   * This method is called to extract the configuration from user configuration for all columns.
   */
  Map<String, C> deserialize(TableConfig tableConfig, Schema schema);

  /**
   * Returns a new {@link ColumnConfigDeserializer} that will merge results from the receiver deserializer and the one
   * received as parameter, failing if a column is defined in both.
   */
  default ColumnConfigDeserializer<C> withExclusiveAlternative(ColumnConfigDeserializer<C> alternative) {
    return new MergedColumnConfigDeserializer<>(MergedColumnConfigDeserializer.OnConflict.FAIL, this, alternative);
  }

  /**
   * Returns a new {@link ColumnConfigDeserializer} that will merge results from the receiver deserializer and the one
   * received as parameter, giving priority to the ones in the receiver if a column is defined in both.
   */
  default ColumnConfigDeserializer<C> withFallbackAlternative(ColumnConfigDeserializer<C> alternative) {
    return new MergedColumnConfigDeserializer<>(MergedColumnConfigDeserializer.OnConflict.PICK_FIRST, this,
        alternative);
  }

  static <C extends IndexConfig> ColumnConfigDeserializer<C> onlyIf(Predicate<TableConfig> predicate,
      ColumnConfigDeserializer<C> delegate) {
    return ((tableConfig, schema) -> predicate.test(tableConfig)
        ? delegate.deserialize(tableConfig, schema)
        : Collections.emptyMap());
  }
}
