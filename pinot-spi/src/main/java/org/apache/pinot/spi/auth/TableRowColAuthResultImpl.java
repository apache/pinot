package org.apache.pinot.spi.auth;

import java.util.List;
import java.util.Map;
import java.util.Optional;


public class TableRowColAuthResultImpl implements TableRowColAuthResult {

  private static final TableRowColAuthResult UNRESTRICTED = new TableRowColAuthResultImpl();

  private Map<String, List<String>> _rlsFilters;
  private Map<String, List<String>> _visibleCols;
  private Map<String, List<String>> _maskedCols;

  public TableRowColAuthResultImpl() {

  }

  public TableRowColAuthResultImpl(Map<String, List<String>> maskedCols, Map<String, List<String>> visibleCols,
      Map<String, List<String>> rlsFilters) {
    _rlsFilters = rlsFilters;
    _maskedCols = maskedCols;
    _visibleCols = visibleCols;
  }

  public TableRowColAuthResult setVisibleCols(Map<String, List<String>> visibleCols) {
    _visibleCols = visibleCols;
    return this;
  }

  @Override
  public TableRowColAuthResult setMaskedCols(Map<String, List<String>> maskedCols) {
    _maskedCols = maskedCols;
    return this;
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

  @Override
  public Optional<Map<String, List<String>>> visibleColumns() {
    return Optional.ofNullable(_visibleCols);
  }

  @Override
  public Optional<Map<String, List<String>>> maskedColumns() {
    return Optional.ofNullable(_maskedCols);
  }

  public static TableRowColAuthResult unrestricted() {
    return UNRESTRICTED;
  }
}
