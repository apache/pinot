package org.apache.pinot.query.mailbox;

import java.util.TreeMap;
import org.apache.pinot.query.QueryEnvironmentTestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


public abstract class GrpcMailboxServiceTestBase {
  protected int MAILBOX_TEST_SIZE = 2;
  protected TreeMap<Integer, GrpcMailboxService> _mailboxServices = new TreeMap<>();

  @BeforeClass
  public void setUp()
      throws Exception {
    for (int i = 0; i < MAILBOX_TEST_SIZE; i++) {
      int availablePort = QueryEnvironmentTestUtils.getAvailablePort();
      GrpcMailboxService grpcMailboxService = new GrpcMailboxService(availablePort);
      grpcMailboxService.start();
      _mailboxServices.put(availablePort, grpcMailboxService);
    }
  }

  @AfterClass
  public void tearDown() {
    for (GrpcMailboxService service : _mailboxServices.values()) {
      service.shutdown();
    }
  }
}
