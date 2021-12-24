package org.apache.pinot.query.runtime.mailbox;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import java.util.TreeMap;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.BeforeClass;


public abstract class GrpcMailboxServiceTestBase {
  protected int MAILBOX_TEST_SIZE = 2;
  protected TreeMap<Integer, GrpcMailboxService> _mailboxServices = new TreeMap<>();

  @BeforeClass
  public void startUp()
      throws Exception {
    for (int i = 0; i < MAILBOX_TEST_SIZE; i++) {
      int availablePort = getAvailablePort();
      GrpcMailboxService grpcMailboxService = new GrpcMailboxService(
          new PinotConfiguration(Map.of(CommonConstants.Server.CONFIG_OF_GRPC_PORT, availablePort)));
      grpcMailboxService.start();
      _mailboxServices.put(availablePort, grpcMailboxService);
    }
  }

  public void tearDown() {
    for (GrpcMailboxService service : _mailboxServices.values()) {
      service.shutdown();
    }
  }

  protected static int getAvailablePort() {
    try {
      try (ServerSocket socket = new ServerSocket(0)) {
        return socket.getLocalPort();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to find an available port to use", e);
    }
  }
}
