package org.apache.pinot.client;

public class DummyPinotClientTransportFactory implements PinotClientTransportFactory {
  private PinotClientTransport _dummyPinotClientTransport;

  public DummyPinotClientTransportFactory(PinotClientTransport pinotClientTransport ) {
    _dummyPinotClientTransport = pinotClientTransport;
  }

  @Override
  public PinotClientTransport buildTransport() {
    return _dummyPinotClientTransport;
  }
}
