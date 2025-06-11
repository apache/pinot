package org.apache.pinot.spi.auth;

import java.util.List;
import java.util.Optional;


public class TableRowColAuthResultImpl implements TableRowColAuthResult {

  private static final TableRowColAuthResult UNRESTRICTED = new TableRowColAuthResultImpl();

  private List<String> _rlsFilters;

  public TableRowColAuthResultImpl() {
  }

  public TableRowColAuthResultImpl(List<String> rlsFilters) {
    _rlsFilters = rlsFilters;
  }

  @Override
  public TableRowColAuthResult setRLSFilters(List<String> rlsFilters) {
    _rlsFilters = rlsFilters;
    return this;
  }

  @Override
  public Optional<List<String>> getRLSFilters() {
    return Optional.ofNullable(_rlsFilters);
  }

  public static TableRowColAuthResult unrestricted() {
    return UNRESTRICTED;
  }
}
