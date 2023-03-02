package org.apache.pinot.segment.spi.index;

import java.io.IOException;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class FieldIndexConfigsTest {

  @Test
  public void testToString()
      throws IOException {
    IndexType index1 = Mockito.mock(IndexType.class);
    Mockito.when(index1.getId()).thenReturn("index1");
    Mockito.when(index1.serialize(Mockito.any())).thenCallRealMethod();
    IndexConfig indexConf = new IndexConfig(true);
    FieldIndexConfigs fieldIndexConfigs = new FieldIndexConfigs.Builder()
        .add(index1, indexConf)
        .build();

    Assert.assertEquals(fieldIndexConfigs.toString(), "{\"index1\":"
        + JsonUtils.objectToString(indexConf) + "}");
  }
}
