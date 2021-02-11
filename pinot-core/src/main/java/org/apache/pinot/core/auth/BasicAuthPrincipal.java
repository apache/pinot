package org.apache.pinot.core.auth;

import java.util.Set;


/**
 * Container object for basic auth principal
 */
public class BasicAuthPrincipal {
  private final String _name;
  private final String _token;
  private final Set<String> _tables;
  private final Set<String> _permissions;

  public BasicAuthPrincipal(String name, String token, Set<String> tables, Set<String> permissions) {
    this._name = name;
    this._token = token;
    this._tables = tables;
    this._permissions = permissions;
  }

  public String getName() {
    return _name;
  }

  public String getToken() {
    return _token;
  }

  public boolean hasTable(String tableName) {
    return _tables.isEmpty() || _tables.contains(tableName);
  }

  public boolean hasPermission(String permission) {
    return _permissions.isEmpty() || _permissions.contains(permission);
  }
}
