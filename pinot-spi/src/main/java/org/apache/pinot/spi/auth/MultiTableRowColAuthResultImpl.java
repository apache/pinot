package org.apache.pinot.spi.auth;

import java.util.List;
import java.util.Map;
import java.util.Optional;


public class MultiTableRowColAuthResultImpl implements MultiTableRowColAuthResult {

  private Map<String, List<String>> _rlsFilters;

  public MultiTableRowColAuthResultImpl(Map<String, List<String>> rlsFilters) {
    _rlsFilters = rlsFilters;
  }

  @Override
  public Optional<List<String>> getRLSFilterForTable(String tableName) {
    return Optional.ofNullable(_rlsFilters.get(tableName));
  }
}
