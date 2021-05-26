package org.apache.pinot.controller.api;

import org.apache.pinot.controller.helix.ControllerTest;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;


/**
 * @author Harish Shankar 
 */
public class PinotControllerTaskRestletResourceTest extends ControllerTest {
  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);
  }



}
