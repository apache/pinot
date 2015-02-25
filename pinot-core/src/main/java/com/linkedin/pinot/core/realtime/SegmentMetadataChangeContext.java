package com.linkedin.pinot.core.realtime;

public class SegmentMetadataChangeContext {

  String pathChanged;
  /**
   * can be CREATED, DELETED, DATA_CHANGED. We can do enum here, but this is for information only.
   * Avoid using this in logic, its hard to code against delta changes. Better to have implementation that is idempotent
   */
  String changeType;
}
