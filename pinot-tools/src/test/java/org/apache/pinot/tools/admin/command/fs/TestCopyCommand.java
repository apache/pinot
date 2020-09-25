package org.apache.pinot.tools.admin.command.fs;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.testng.annotations.Test;
import org.testng.Assert;


public class TestCopyCommand {
  @Test()
  public void testExecute()
      throws Exception {
    CopyCommand command = new CopyCommand();
    command.setSourceURI(new URI("mock://source"));
    command.setDestinationURI(new URI("mock://destination"));

    Map<String, Object> props = new HashMap<>();
    props.put("class.mock", "org.apache.pinot.tools.admin.command.fs.MockPinotFS");
    command.setConfiguration(props);

    Assert.assertTrue(command.execute());
  }

  @Test()
  public void testDifferentSchemes()
      throws Exception {
    CopyCommand command = new CopyCommand();
    command.setSourceURI(new URI("mock://source"));
    command.setDestinationURI(new URI("file://destination"));

    Map<String, Object> props = new HashMap<>();
    props.put("class.mock", "org.apache.pinot.tools.admin.command.fs.MockPinotFS");
    command.setConfiguration(props);

    Assert.assertFalse(command.execute());
  }
}
