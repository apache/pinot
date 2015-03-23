package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.UUID;

public class TestStarTreeRecordStoreFactoryFixedImpl
{
  public static void main(String[] args) throws Exception
  {
    StarTreeConfig config = StarTreeConfig.decode(new FileInputStream(
            "/Users/gbrandt/IdeaProjects/thirdeye-mirror/thirdeye-server/target/test-classes/abook-config.yml"));
    StarTreeRecordStoreFactory factory = new StarTreeRecordStoreFactoryDefaultImpl();
    factory.init(new File("/tmp/thirdeye/abook/data"), config, null);
    UUID id = UUID.fromString("c5872fda-501d-415a-9fd0-332fec4a0f2f");
    StarTreeRecordStore recordStore = factory.createRecordStore(id);

    for (StarTreeRecord record : recordStore)
    {
      System.out.println(record);
    }
  }
}
