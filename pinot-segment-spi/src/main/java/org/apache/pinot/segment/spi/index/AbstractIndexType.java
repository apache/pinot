package org.apache.pinot.segment.spi.index;

import java.util.Map;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


public abstract class AbstractIndexType<C extends IndexConfig, IR extends IndexReader, IC extends IndexCreator>
    implements IndexType<C, IR, IC> {

  private ColumnConfigDeserializer<C> _deserializer;

  protected abstract ColumnConfigDeserializer<C> getDeserializer();

  @Override
  public Map<String, C> getConfig(TableConfig tableConfig, Schema schema) {
    if (_deserializer == null) {
      _deserializer = getDeserializer();
    }
    return _deserializer.deserialize(tableConfig, schema);
  }
}
