package org.apache.pinot.spi.auth;

import java.util.List;
import java.util.Optional;


/**
 * This interfaces carries the RLS/CLS filter for a particular table
 */
public interface TableRowColAuthResult {
  /**
   * Returns the RLS filters associated with a particular table. RLS filters are defined as a list.
   * @return optional of the RLS filters. Empty optional is there are no RLS filters defined on this table
   */
  Optional<List<String>> getRLSFilters();
  Optional<List<String>> visibleColumns();
  Optional<List<String>> maskedColumns();
}
