package org.apache.pinot.spi.auth;

import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * This interfaces carries the RLS/CLS filter for a particular table
 */
public interface TableRowColAuthResult {
  /**
   * Returns the RLS filters associated with a particular table. RLS filters are defined as a list.
   * @return optional of the RLS filters. Empty optional is there are no RLS filters defined on this table
   */
  Optional<Map<String, List<String>>> getRLSFilters();

  Optional<Map<String, List<String>>> visibleColumns();

  Optional<Map<String, List<String>>> maskedColumns();

  TableRowColAuthResult setRLSFilters(Map<String, List<String>> rlsFilters);

  TableRowColAuthResult setVisibleCols(Map<String, List<String>> visibleCols);

  TableRowColAuthResult setMaskedCols(Map<String, List<String>> maskedCols);
}
