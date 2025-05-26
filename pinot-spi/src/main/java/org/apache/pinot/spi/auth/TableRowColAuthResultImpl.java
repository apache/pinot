package org.apache.pinot.spi.auth;

import java.util.List;
import java.util.Optional;


public class TableRowColAuthResultImpl implements TableRowColAuthResult {

  private static final TableRowColAuthResult NO_FILTERS = new TableRowColAuthResultImpl();

  private List<String> _rlsFilters;

  public TableRowColAuthResultImpl() {

  }

  public TableRowColAuthResultImpl(List<String> rlsFilters) {
    _rlsFilters = rlsFilters;
  }

  @Override
  public Optional<List<String>> getRLSFilters() {
    return Optional.ofNullable(_rlsFilters);
  }

  public static TableRowColAuthResult noRowColFilters() {
    return NO_FILTERS;
  }
}
