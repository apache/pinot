package org.apache.pinot.spi.config.provider;

import java.util.List;
import org.apache.pinot.spi.data.Schema;


/**
 * Interface for a listener on schema changes
 */
public interface SchemaChangeListener {

  /**
   * The callback to be invoked on schema changes
   * @param schemaList the entire list of schemas in the cluster
   */
  void onChange(List<Schema> schemaList);
}
