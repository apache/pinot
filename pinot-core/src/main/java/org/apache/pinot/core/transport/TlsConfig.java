package org.apache.pinot.core.transport;

/**
 * Container object for TLS/SSL configuration of pinot clients and servers (netty, grizzly, etc.)
 */
public class TlsConfig {
  private boolean _enabled;
  private boolean _clientAuth;
  private String _keyStorePath;
  private String _keyStorePassword;
  private String _trustStorePath;
  private String _trustStorePassword;

  public boolean isEnabled() {
    return _enabled;
  }

  public void setEnabled(boolean enabled) {
    _enabled = enabled;
  }

  public boolean isClientAuth() {
    return _clientAuth;
  }

  public void setClientAuth(boolean clientAuth) {
    _clientAuth = clientAuth;
  }

  public String getKeyStorePath() {
    return _keyStorePath;
  }

  public void setKeyStorePath(String keyStorePath) {
    _keyStorePath = keyStorePath;
  }

  public String getKeyStorePassword() {
    return _keyStorePassword;
  }

  public void setKeyStorePassword(String keyStorePassword) {
    _keyStorePassword = keyStorePassword;
  }

  public String getTrustStorePath() {
    return _trustStorePath;
  }

  public void setTrustStorePath(String trustStorePath) {
    _trustStorePath = trustStorePath;
  }

  public String getTrustStorePassword() {
    return _trustStorePassword;
  }

  public void setTrustStorePassword(String trustStorePassword) {
    _trustStorePassword = trustStorePassword;
  }
}
