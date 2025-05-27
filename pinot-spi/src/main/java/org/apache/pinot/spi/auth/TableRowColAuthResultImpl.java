package org.apache.pinot.spi.auth;

import java.util.List;
import java.util.Optional;
import lombok.Builder;


public class TableRowColAuthResultImpl implements TableRowColAuthResult {

  private static final TableRowColAuthResult UNRESTRICTED = new TableRowColAuthResultImpl();

  private List<String> _rlsFilters;
  private List<String> _visibleCols;
  private List<String> _maskedCols;

  public TableRowColAuthResultImpl() {

  }

  public TableRowColAuthResultImpl(List<String> rlsFilters, List<String> visibleCols, List<String> maskedCols) {
    _rlsFilters = rlsFilters;
    _visibleCols = visibleCols;
    _maskedCols = maskedCols;
  }

  @Override
  public Optional<List<String>> getRLSFilters() {
    return Optional.ofNullable(_rlsFilters);
  }

  @Override
  public Optional<List<String>> visibleColumns() {
    return Optional.ofNullable(_visibleCols);
  }

  @Override
  public Optional<List<String>> maskedColumns() {
    return Optional.ofNullable(_maskedCols);
  }

  public static TableRowColAuthResult unrestricted() {
    return UNRESTRICTED;
  }
}
