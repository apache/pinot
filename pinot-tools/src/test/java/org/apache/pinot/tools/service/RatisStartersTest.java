/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools.service;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.coordination.MetadataRpcClient;
import org.apache.pinot.common.coordination.NativeMetadataConfig;
import org.apache.pinot.common.coordination.ServingPermitGuard.ClockReading;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.controller.ControllerStarter;
import org.apache.pinot.controller.coordination.RatisControllerProvider;
import org.apache.pinot.coordination.ratis.RatisMetadataStore.MembershipClockSettings;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Policy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ResultStatus;
import org.apache.pinot.coordination.ratis.transport.ControllerAccessPolicy;
import org.apache.pinot.coordination.ratis.transport.ControllerAccessPolicy.ControllerIdentity;
import org.apache.pinot.coordination.ratis.transport.ParticipantAccessPolicy;
import org.apache.pinot.coordination.ratis.transport.ParticipantAccessPolicy.ParticipantIdentity;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Exercises normal service-manager dispatch with real voters and mTLS. No ZooKeeper is started; configured query
/// ports remain available because these native starters expose metadata and installation membership only.
public class RatisStartersTest {
  private static final String PREFIX = NativeMetadataConfig.PREFIX;
  private static final String BROKER = "Broker_native";
  private static final String SERVER = "Server_native";

  @Test(timeOut = 45_000)
  public void testFailedControllerStartReleasesOwnership() throws Exception {
    ControllerStarter invalid = new ControllerStarter();
    assertThrows(IllegalArgumentException.class,
        () -> invalid.init(new PinotConfiguration(Map.of("pinot.coordination.backend", "RATIS"))));
    invalid.stop();
    assertThrows(IllegalStateException.class, invalid::start);

    Path directory = Files.createTempDirectory("pinot-ratis-failed-starter-");
    String instance = "Controller_ratis_failure-0";
    ServiceStatus.setServiceStatusCallback(instance,
        new ServiceStatus.LifecycleServiceStatusCallback(() -> false, () -> false));
    try {
      CertificateFiles certificate = certificate(directory, "controller");
      int[] ports = ports(4);
      Map<String, Object> config = tls(UUID.randomUUID().toString(), certificate, certificate.certificate());
      config.put(PREFIX + "clusterId", UUID.randomUUID().toString());
      config.put(PREFIX + "id", "failure-0");
      config.put(PREFIX + "voters", "failure-0@127.0.0.1:" + ports[0] + ",failure-1@127.0.0.1:" + ports[1]
          + ",failure-2@127.0.0.1:" + ports[2]);
      config.put(PREFIX + "dataDir", directory.resolve("voter").toString());
      config.put(PREFIX + "bootstrap", true);
      config.put(PREFIX + "metadata.host", "127.0.0.1");
      config.put(PREFIX + "metadata.port", ports[3]);
      config.put(PREFIX + "controller.provider.class", TestProvider.class.getName());
      config.put(PREFIX + "controller.startup.timeoutMs", 250L);
      for (int attempt = 0; attempt < 2; attempt++) {
        ControllerStarter starter = new ControllerStarter();
        try {
          starter.init(new PinotConfiguration(config));
          IOException failure = expectThrows(IOException.class, starter::start);
          assertTrue(failure.getMessage().contains("Timed out initializing"), failure.getMessage());
        } finally {
          starter.stop();
        }
        assertEquals(ServiceStatus.getServiceStatus(instance), ServiceStatus.Status.GOOD,
            "A failed startup must not remove another runtime's callback");
        config.put(PREFIX + "bootstrap", false);
      }
      try (ServerSocket released = new ServerSocket(ports[0])) {
        assertTrue(released.isBound(), "Failed startup must release its voter listener");
      }
    } finally {
      ServiceStatus.removeServiceStatusCallback(instance);
      FileUtils.deleteDirectory(directory.toFile());
    }
  }

  @Test(timeOut = 120_000)
  public void testNormalStartersWithoutZooKeeper() throws Exception {
    Path directory = Files.createTempDirectory("pinot-ratis-starters-");
    PinotServiceManager manager = new PinotServiceManager("unreachable-zookeeper.invalid:1", "unused", -1);
    ExecutorService starters = Executors.newFixedThreadPool(3);
    try {
      CertificateFiles controller = certificate(directory, "controller");
      CertificateFiles broker = certificate(directory, "broker");
      CertificateFiles server = certificate(directory, "server");
      Path trust = directory.resolve("clients.pem");
      Files.writeString(trust, Files.readString(controller.certificate()) + Files.readString(broker.certificate())
          + Files.readString(server.certificate()));
      int[] ports = ports(9);
      String history = UUID.randomUUID().toString();
      String cluster = UUID.randomUUID().toString();
      String voters = "voter-0@127.0.0.1:" + ports[0] + ",voter-1@127.0.0.1:" + ports[1]
          + ",voter-2@127.0.0.1:" + ports[2];
      List<Map<String, Object>> controllers = new ArrayList<>();
      List<Future<String>> starts = new ArrayList<>();
      manager.start();
      for (int i = 0; i < 3; i++) {
        Map<String, Object> config = tls(history, controller, trust);
        config.put(PREFIX + "clusterId", cluster);
        config.put(PREFIX + "id", "voter-" + i);
        config.put(PREFIX + "voters", voters);
        config.put(PREFIX + "dataDir", directory.resolve("voter-" + i).toString());
        config.put(PREFIX + "bootstrap", true);
        config.put(PREFIX + "metadata.host", "127.0.0.1");
        config.put(PREFIX + "metadata.port", ports[3 + i]);
        config.put(PREFIX + "controller.provider.class", TestProvider.class.getName());
        config.put("test.controllerFingerprint", controller.fingerprint());
        config.put("test.brokerFingerprint", broker.fingerprint());
        config.put("test.serverFingerprint", server.fingerprint());
        controllers.add(config);
        starts.add(starters.submit(() -> manager.startRole(ServiceRole.CONTROLLER, config)));
      }
      for (int i = 0; i < starts.size(); i++) {
        assertEquals(starts.get(i).get(45, TimeUnit.SECONDS), "Controller_ratis_voter-" + i);
        assertEquals(ServiceStatus.getServiceStatus("Controller_ratis_voter-" + i), ServiceStatus.Status.GOOD);
        assertTrue(ServiceStatus.getStatusDescription("Controller_ratis_voter-" + i).contains("HTTP"));
      }

      String endpoints = "127.0.0.1:" + ports[3] + ",127.0.0.1:" + ports[4] + ",127.0.0.1:" + ports[5];
      Map<String, Object> brokerConfig = tls(history, broker, controller.certificate());
      brokerConfig.put(PREFIX + "participantId", BROKER);
      brokerConfig.put(PREFIX + "metadata.endpoints", endpoints);
      brokerConfig.put(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, ports[6]);
      assertEquals(manager.startRole(ServiceRole.BROKER, brokerConfig), BROKER);

      Map<String, Object> serverConfig = tls(history, server, controller.certificate());
      serverConfig.put(PREFIX + "participantId", SERVER);
      serverConfig.put(PREFIX + "metadata.endpoints", endpoints);
      serverConfig.put("pinot.server.instance.id", SERVER);
      serverConfig.put(CommonConstants.Server.CONFIG_OF_INSTANCE_DATA_DIR, directory.resolve("server-data").toString());
      serverConfig.put(CommonConstants.Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR,
          directory.resolve("server-tar").toString());
      serverConfig.put(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT, ports[7]);
      serverConfig.put(CommonConstants.Server.CONFIG_OF_ADMIN_API_PORT, ports[8]);
      assertEquals(manager.startRole(ServiceRole.SERVER, serverConfig), SERVER);

      try (MetadataRpcClient metadata = client(ports[4], controller, controller.certificate());
          MetadataRpcClient brokerClient = client(ports[4], broker, controller.certificate());
          MetadataRpcClient serverClient = client(ports[4], server, controller.certificate())) {
        assertEquals(metadata.read(MetadataRpc.ReadRequest.newBuilder().setHistoryId(history)
            .setRevision(MetadataRpc.Empty.getDefaultInstance()).build()).getStatus(), ResultStatus.OK);
        MetadataRpc.MembershipReadRequest read = MetadataRpc.MembershipReadRequest.newBuilder()
            .setHistoryId(history).build();
        MetadataRpc.ParticipantState brokerState = brokerClient.membershipRead(read).getParticipant();
        MetadataRpc.ParticipantState serverState = serverClient.membershipRead(read).getParticipant();
        assertFalse(brokerState.getReady(), "Membership alone must not open broker routing");
        assertTrue(serverState.getReady(), "The server has installed its complete own-session image");
        assertEquals(brokerState.getCapabilitiesCount(), 0);
        assertEquals(serverState.getCapabilitiesCount(), 0);
        assertEquals(brokerState.getEndpointsCount(), 0);
        assertEquals(serverState.getEndpointsCount(), 0);
        assertEquals(ServiceStatus.getServiceStatus(BROKER), ServiceStatus.Status.BAD);
        assertEquals(ServiceStatus.getServiceStatus(SERVER), ServiceStatus.Status.GOOD);
        for (int i = 6; i < ports.length; i++) {
          try (ServerSocket unused = new ServerSocket(ports[i])) {
            assertTrue(unused.isBound(), "Native query/admin listener must remain closed");
          }
        }

        assertTrue(manager.stopPinotInstanceById(SERVER));
        assertTrue(serverClient.membershipRead(read).getParticipant().getRetired());
        assertEquals(manager.startRole(ServiceRole.SERVER, serverConfig), SERVER);
        MetadataRpc.ParticipantToken replacement = serverClient.membershipRead(read).getParticipant().getParticipant();
        assertEquals(replacement.getRegistrationEpoch(), serverState.getParticipant().getRegistrationEpoch() + 1);
        assertNotEquals(replacement.getIncarnation(), serverState.getParticipant().getIncarnation());

        assertTrue(manager.stopPinotInstanceById("Controller_ratis_voter-0"));
        controllers.getFirst().put(PREFIX + "bootstrap", false);
        controllers.getFirst().put("test.bootstrapPermitMillis", 2000L);
        assertEquals(manager.startRole(ServiceRole.CONTROLLER, controllers.getFirst()), "Controller_ratis_voter-0");
        assertEquals(metadata.membershipRead(read).getControl().getPolicy().getPermitLifetimeMillis(), 1000L,
            "Restart must retain committed policy instead of resetting it from bootstrap configuration");
      }
    } finally {
      starters.shutdownNow();
      starters.awaitTermination(10, TimeUnit.SECONDS);
      // Participants retire while a quorum remains available; the manager's arbitrary iteration order is unsuitable.
      manager.stopPinotInstanceById(BROKER);
      manager.stopPinotInstanceById(SERVER);
      manager.stopAll();
      FileUtils.deleteDirectory(directory.toFile());
    }
  }

  private static Map<String, Object> tls(String history, CertificateFiles certificate, Path trust) {
    Map<String, Object> config = new HashMap<>();
    config.put("pinot.coordination.backend", "RATIS");
    config.put(PREFIX + "historyId", history);
    config.put(PREFIX + "tls.certChain", certificate.certificate().toString());
    config.put(PREFIX + "tls.privateKey", certificate.key().toString());
    config.put(PREFIX + "tls.trustCerts", trust.toString());
    return config;
  }

  private static MetadataRpcClient client(int port, CertificateFiles certificate, Path trust) throws IOException {
    return new MetadataRpcClient("127.0.0.1", port, certificate.certificate(), certificate.key(), trust, 5000);
  }

  private static int[] ports(int count) throws IOException {
    List<ServerSocket> reservations = new ArrayList<>();
    int[] ports = new int[count];
    try {
      for (int i = 0; i < count; i++) {
        ServerSocket socket = new ServerSocket(0);
        reservations.add(socket);
        ports[i] = socket.getLocalPort();
      }
      return ports;
    } finally {
      for (ServerSocket socket : reservations) {
        socket.close();
      }
    }
  }

  private static CertificateFiles certificate(Path directory, String name) throws Exception {
    Path keyStoreFile = directory.resolve(name + ".p12");
    Path certificateFile = directory.resolve(name + ".pem");
    Path keyFile = directory.resolve(name + "-key.pem");
    String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
    Process process = new ProcessBuilder(keytool, "-genkeypair", "-alias", "test", "-keyalg", "EC", "-groupname",
        "secp256r1", "-dname", "CN=" + name, "-ext", "SAN=DNS:localhost,IP:127.0.0.1", "-ext",
        "EKU=serverAuth,clientAuth", "-validity", "2", "-storetype", "PKCS12", "-keystore", keyStoreFile.toString(),
        "-storepass", "test-password", "-keypass", "test-password", "-noprompt").redirectErrorStream(true).start();
    if (!process.waitFor(30, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      throw new IOException("Timed out generating TLS test certificate");
    }
    assertEquals(process.exitValue(), 0, new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    try (var input = Files.newInputStream(keyStoreFile)) {
      keyStore.load(input, "test-password".toCharArray());
    }
    X509Certificate certificate = (X509Certificate) keyStore.getCertificate("test");
    Files.writeString(certificateFile, pem("CERTIFICATE", certificate.getEncoded()));
    Files.writeString(keyFile, pem("PRIVATE KEY", keyStore.getKey("test", "test-password".toCharArray()).getEncoded()));
    return new CertificateFiles(certificateFile, keyFile, ControllerAccessPolicy.fingerprint(certificate));
  }

  private static String pem(String kind, byte[] bytes) {
    return "-----BEGIN " + kind + "-----\n" + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(bytes)
        + "\n-----END " + kind + "-----\n";
  }

  private record CertificateFiles(Path certificate, Path key, String fingerprint) {
  }

  /// Test-only clock assumptions and exact certificate bindings; this is not a deployment qualification provider.
  public static final class TestProvider implements RatisControllerProvider {
    private String _controller;
    private String _broker;
    private String _server;
    private long _permitMillis;

    @Override
    public void init(PinotConfiguration config) {
      _controller = config.getProperty("test.controllerFingerprint");
      _broker = config.getProperty("test.brokerFingerprint");
      _server = config.getProperty("test.serverFingerprint");
      _permitMillis = config.getProperty("test.bootstrapPermitMillis", 1000L);
    }

    @Override
    public MembershipClockSettings membershipClockSettings() {
      return new MembershipClockSettings(() -> new ClockReading(System.nanoTime(), 0, true), 1_000_000, 0);
    }

    @Override
    public ControllerAccessPolicy controllerAccessPolicy() {
      return fingerprint -> fingerprint.equals(_controller) ? new ControllerIdentity("test-controller", "test-scope")
          : null;
    }

    @Override
    public ParticipantAccessPolicy participantAccessPolicy() {
      return fingerprint -> {
        if (fingerprint.equals(_broker)) {
          return new ParticipantIdentity(BROKER, Role.BROKER, Set.of());
        }
        if (fingerprint.equals(_server)) {
          return new ParticipantIdentity(SERVER, Role.SERVER, Set.of());
        }
        return fingerprint.equals(_controller) ? new ParticipantIdentity("Controller_test", Role.CONTROLLER, Set.of())
            : null;
      };
    }

    @Override
    public Policy bootstrapPolicy() {
      return new Policy(1, 100, 1000, _permitMillis, 1000, 100_000);
    }
  }
}
