package org.apache.pinot.spi.stream;

public class PartitionLagInfo {
  private String _offsetLag = "UNKNOWN";

  public static PartitionLagInfo EMPTY() {
    return new PartitionLagInfo();
  }

  public String getOffsetLag() {
    return _offsetLag;
  }

  public void setOffsetLag(String offsetLag) {
    _offsetLag = offsetLag;
  }
}
