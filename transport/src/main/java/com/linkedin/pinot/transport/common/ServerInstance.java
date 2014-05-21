package com.linkedin.pinot.transport.common;

/**
 * Service abstraction. A service is identified by its hostname and port.
 * Protocol is always assumed to be TCP
 */
public class ServerInstance
{
  /** Hostname where the service is running **/
  private final String _hostname;

  /** Service Port **/
  private final int _port;

  public ServerInstance(String hostname, int port)
  {
    super();
    _hostname = hostname;
    _port = port;
  }

  public String getHostname()
  {
    return _hostname;
  }

  public int getPort()
  {
    return _port;
  }

  @Override
  public int hashCode()
  {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((_hostname == null) ? 0 : _hostname.hashCode());
    result = (prime * result) + _port;
    return result;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj)
    {
      return true;
    }
    if (obj == null)
    {
      return false;
    }
    if (getClass() != obj.getClass())
    {
      return false;
    }
    ServerInstance other = (ServerInstance) obj;
    if (_hostname == null)
    {
      if (other._hostname != null)
      {
        return false;
      }
    }
    else if (!_hostname.equals(other._hostname))
    {
      return false;
    }
    if (_port != other._port)
    {
      return false;
    }
    return true;
  }
}

