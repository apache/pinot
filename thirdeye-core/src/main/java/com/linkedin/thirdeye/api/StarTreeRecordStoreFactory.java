package com.linkedin.thirdeye.api;

import java.util.UUID;

public interface StarTreeRecordStoreFactory
{
  StarTreeRecordStore createRecordStore(UUID nodeId);
}
