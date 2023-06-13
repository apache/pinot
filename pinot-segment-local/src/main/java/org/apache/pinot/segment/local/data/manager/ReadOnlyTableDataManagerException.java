package org.apache.pinot.segment.local.data.manager;

public class ReadOnlyTableDataManagerException extends UnsupportedOperationException {
  public ReadOnlyTableDataManagerException(String readOnlyReason, String operation) {
    super("cannot " + operation + " during " + readOnlyReason);
  }
}
