package org.apache.pinot.spi.auth;

import java.util.List;
import java.util.Optional;


public class BasicAuthTableRLSCLSAuthResultImpl implements TableRLSCLSAuthResult {

  private static final TableRLSCLSAuthResult SUCCESS = new BasicAuthTableRLSCLSAuthResultImpl();

  private List<String> _rlsFilters;

  public BasicAuthTableRLSCLSAuthResultImpl() {

  }

  public BasicAuthTableRLSCLSAuthResultImpl(List<String> rlsFilters) {
    _rlsFilters = rlsFilters;
  }

  @Override
  public Optional<List<String>> getRLSFilters() {
    return Optional.ofNullable(_rlsFilters);
  }

  public static TableRLSCLSAuthResult noRLSCLSFilters() {
    return SUCCESS;
  }
}
