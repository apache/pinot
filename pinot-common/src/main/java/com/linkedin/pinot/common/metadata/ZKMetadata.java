package com.linkedin.pinot.common.metadata;

import org.apache.helix.ZNRecord;


public interface ZKMetadata {
  ZNRecord toZNRecord();
}
