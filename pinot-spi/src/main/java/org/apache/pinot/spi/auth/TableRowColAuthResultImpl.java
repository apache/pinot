package org.apache.pinot.spi.auth;

import java.util.List;
import java.util.Map;
import java.util.Optional;


public class TableRowColAuthResultImpl implements TableRowColAuthResult {

  private static final TableRowColAuthResult UNRESTRICTED = new TableRowColAuthResultImpl();

  private Map<String, List<String>> _rlsFilters;

  public TableRowColAuthResultImpl() {

  }

  public TableRowColAuthResultImpl(Map<String, List<String>> rlsFilters) {
    _rlsFilters = rlsFilters;
  }

  @Override
  public TableRowColAuthResult setRLSFilters(Map<String, List<String>> rlsFilters) {
    _rlsFilters = rlsFilters;
    return this;
  }

  @Override
  public Optional<Map<String, List<String>>> getRLSFilters() {
    return Optional.ofNullable(_rlsFilters);
  }

  public static TableRowColAuthResult unrestricted() {
    return UNRESTRICTED;
  }
}