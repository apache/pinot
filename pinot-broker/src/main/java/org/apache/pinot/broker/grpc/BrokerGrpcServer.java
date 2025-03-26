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
package org.apache.pinot.broker.grpc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerTransportFilter;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;
import nl.altindag.ssl.SSLFactory;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandler;
import org.apache.pinot.common.compression.CompressionFactory;
import org.apache.pinot.common.compression.Compressor;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.proto.PinotQueryBrokerGrpc;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.common.utils.tls.RenewableTlsUtils;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.trace.DefaultRequestContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BrokerGrpcServer extends PinotQueryBrokerGrpc.PinotQueryBrokerImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerGrpcServer.class);

  private final String _brokerId;
  private final Server _server;
  private final int _grpcPort;
  private final int _secureGrpcPort;
  private final GrpcConfig _queryClientConfig;
  private final BrokerMetrics _brokerMetrics;
  private final BrokerRequestHandler _brokerRequestHandler;

  // Filter to keep track of gRPC connections.
  private class BrokerGrpcTransportFilter extends ServerTransportFilter {
    @Override
    public Attributes transportReady(Attributes transportAttrs) {
      LOGGER.info("gRPC transportReady: REMOTE_ADDR {}",
          transportAttrs != null ? transportAttrs.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR) : "null");
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.GRPC_TRANSPORT_READY, 1);
      return super.transportReady(transportAttrs);
    }

    @Override
    public void transportTerminated(Attributes transportAttrs) {
      // transportTerminated can be called without transportReady before it, e.g. handshake fails
      // So, don't emit metrics if transportAttrs is null
      if (transportAttrs != null) {
        LOGGER.info("gRPC transportTerminated: REMOTE_ADDR {}", transportAttrs.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.GRPC_TRANSPORT_TERMINATED, 1);
      }
    }
  }

  public BrokerGrpcServer(PinotConfiguration brokerConf, String brokerId, BrokerMetrics brokerMetrics,
      BrokerRequestHandler brokerRequestHandler) {
    _brokerMetrics = brokerMetrics;
    _grpcPort = brokerConf.getProperty(CommonConstants.Broker.Grpc.KEY_OF_GRPC_PORT, -1);
    _queryClientConfig = createQueryClientConfig(brokerConf);
    LOGGER.info("gRPC query client config: usePlainText {}", _queryClientConfig.isUsePlainText());
    _secureGrpcPort = brokerConf.getProperty(CommonConstants.Broker.Grpc.KEY_OF_GRPC_TLS_PORT, -1);
    if (_secureGrpcPort > 0) {
      try {
        TlsConfig tlsConfig = TlsUtils.extractTlsConfig(brokerConf, CommonConstants.Broker.BROKER_TLS_PREFIX);
        LOGGER.info("Creating Secure gRPC Server in port {}", _secureGrpcPort);
        _server =
            NettyServerBuilder.forPort(_secureGrpcPort).sslContext(buildGRpcSslContext(tlsConfig)).addService(this)
                .addTransportFilter(new BrokerGrpcTransportFilter()).build();
      } catch (Exception e) {
        throw new RuntimeException("Failed to start secure grpcQueryServer", e);
      }
    } else if (_grpcPort > 0) {
      LOGGER.info("Creating plain text gRPC Server in port {}", _grpcPort);
      _server =
          ServerBuilder.forPort(_grpcPort).addService(this).addTransportFilter(new BrokerGrpcTransportFilter()).build();
    } else {
      LOGGER.info("Not creating gRPC Server due to the grpc port is {} and secureGrpcPort is {}", _grpcPort,
          _secureGrpcPort);
      _server = null;
    }
    _brokerId = brokerId;
    _brokerRequestHandler = brokerRequestHandler;
    LOGGER.info("Initialized BrokerGrpcServer on port: {}", _grpcPort);
  }

  public void start() {
    if (_server == null) {
      LOGGER.info("BrokerGrpcServer is not configured, nothing to start");
      return;
    }
    LOGGER.info("Starting BrokerGrpcServer");
    try {
      _server.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdown() {
    if (_server == null) {
      LOGGER.info("BrokerGrpcServer is not running, nothing to shutdown");
      return;
    }
    LOGGER.info("Shutting down BrokerGrpcServer");
    try {
      _server.shutdown().awaitTermination();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void submit(Broker.BrokerRequest request,
      StreamObserver<Broker.BrokerResponse> responseObserver) {
    if (_server == null) {
      LOGGER.info("BrokerGrpcServer is not running, nothing to handle request");
      responseObserver.onError(new RuntimeException("BrokerGrpcServer is not running"));
      return;
    }
    long startTime = System.nanoTime();
    _brokerMetrics.addMeteredGlobalValue(BrokerMeter.GRPC_QUERIES, 1);
    _brokerMetrics.addMeteredGlobalValue(BrokerMeter.GRPC_BYTES_RECEIVED, request.getSerializedSize());
    String query = request.getSql();
    Map<String, String> metadataMap = request.getMetadataMap();
    // convert request.getMetadataMap() to JsonNode
    ObjectNode requestJsonNode = JsonUtils.newObjectNode();
    requestJsonNode.put(CommonConstants.Broker.Request.SQL, query);
    for (Map.Entry<String, String> entry : metadataMap.entrySet()) {
      requestJsonNode.put(entry.getKey(), entry.getValue());
    }

    SqlNodeAndOptions sqlNodeAndOptions;
    try {
      sqlNodeAndOptions = RequestUtils.parseQuery(query, requestJsonNode);
    } catch (Exception e) {
      // Do not log or emit metric here because it is pure user error
      Broker.BrokerResponse errorBlock;
      try {
        errorBlock = Broker.BrokerResponse.newBuilder().setPayload(ByteString.copyFrom(
            new BrokerResponseNative(QueryErrorCode.SQL_PARSING, e.getMessage()).toJsonString().getBytes())).build();
      } catch (IOException ex) {
        responseObserver.onCompleted();
        throw new RuntimeException(ex);
      }
      responseObserver.onNext(errorBlock);
      responseObserver.onCompleted();
      return;
    }

    RequesterIdentity requesterIdentify = GrpcRequesterIdentity.fromRequest(request);
    RequestContext requestContext = new DefaultRequestContext();
    BrokerResponse brokerResponse;
    try {
      brokerResponse =
          _brokerRequestHandler.handleRequest(requestJsonNode, sqlNodeAndOptions, requesterIdentify, requestContext,
              null);
    } catch (Exception e) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.GRPC_QUERY_EXCEPTIONS, 1);
      LOGGER.error("Error handling DQL request:\n{}\nException: {}", requestJsonNode, e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e)
              .asRuntimeException());
      throw new RuntimeException(e);
    }
    ResultTable resultTable = brokerResponse.getResultTable();
    // Handle empty and error block
    if (resultTable == null) {
      if (brokerResponse.getExceptionsSize() > 0) {
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.GRPC_QUERY_EXCEPTIONS, 1);
      }
      Broker.BrokerResponse emptyOrErrorBlock;
      try {
        emptyOrErrorBlock =
            Broker.BrokerResponse.newBuilder().setPayload(ByteString.copyFrom(brokerResponse.toJsonString().getBytes()))
                .build();
      } catch (IOException e) {
        responseObserver.onCompleted();
        throw new RuntimeException(e);
      }
      responseObserver.onNext(emptyOrErrorBlock);
      responseObserver.onCompleted();
      return;
    }
    // Handle query response:
    // First block is metadata
    try {
      Broker.BrokerResponse metadataBlock =
          Broker.BrokerResponse.newBuilder()
              .setPayload(ByteString.copyFrom(brokerResponse.toMetadataJsonString().getBytes()))
              .build();
      responseObserver.onNext(metadataBlock);
    } catch (IOException e) {
      responseObserver.onError(e);
      throw new RuntimeException(e);
    }
    // Second block is data schema
    try {
      Broker.BrokerResponse schemaBlock =
          Broker.BrokerResponse.newBuilder().setPayload(ByteString.copyFrom(resultTable.getDataSchema().toBytes()))
              .build();
      responseObserver.onNext(schemaBlock);
    } catch (IOException e) {
      responseObserver.onError(e);
      throw new RuntimeException(e);
    }
    int blockRowSize =
        Integer.parseInt(metadataMap.getOrDefault(CommonConstants.Broker.Grpc.BLOCK_ROW_SIZE,
            String.valueOf(CommonConstants.Broker.Grpc.DEFAULT_BLOCK_ROW_SIZE)));

    String compressionAlgorithm = metadataMap.getOrDefault(CommonConstants.Broker.Grpc.COMPRESSION,
        CommonConstants.Broker.Grpc.DEFAULT_COMPRESSION);
    Compressor compressor = CompressionFactory.getCompressor(compressionAlgorithm);
    // Multiple response blocks are compressed data rows
    for (int i = 0; i < resultTable.getRows().size(); i += blockRowSize) {
      try {
        int rowSize = 0;
        // Serialize the rows to a byte array
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        for (int j = i; j < Math.min(i + blockRowSize, resultTable.getRows().size()); j++) {
          String rowString = JsonUtils.objectToJsonNode(resultTable.getRows().get(j)).toString();
          byte[] bytesToWrite = rowString.getBytes(StandardCharsets.UTF_8);
          byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(bytesToWrite.length).array());
          byteArrayOutputStream.write(bytesToWrite);
          rowSize += 1;
        }

        // Compress the byte array using the compressor
        byte[] serializedData = byteArrayOutputStream.toByteArray();
        byte[] compressedResultTable = compressor.compress(serializedData);
        int originalSize = serializedData.length;
        int compressedSize = compressedResultTable.length;
        Broker.BrokerResponse dataBlock =
            Broker.BrokerResponse.newBuilder()
                .setPayload(ByteString.copyFrom(compressedResultTable))
                .putMetadata("originalSize", String.valueOf(originalSize))
                .putMetadata("compressedSize", String.valueOf(compressedSize))
                .putMetadata("rowSize", String.valueOf(rowSize))
                .putMetadata("compression", compressionAlgorithm)
                .build();
        responseObserver.onNext(dataBlock);
      } catch (Exception e) {
        responseObserver.onError(e);
        throw new RuntimeException(e);
      }
    }
    responseObserver.onCompleted();
  }

  //TODO: move this method from OSS Pinot class into util, and then re-use this util
  @VisibleForTesting
  static SslContext buildGRpcSslContext(TlsConfig tlsConfig)
      throws Exception {
    LOGGER.info("Building gRPC SSL context with");
    if (tlsConfig.getKeyStorePath() == null) {
      throw new IllegalArgumentException("Must provide key store path for secured gRPC server");
    } else {
      SSLFactory sslFactory =
          RenewableTlsUtils.createSSLFactoryAndEnableAutoRenewalWhenUsingFileStores(tlsConfig);
      // since tlsConfig.getKeyStorePath() is not null, sslFactory.getKeyManagerFactory().get() should not be null
      SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(sslFactory.getKeyManagerFactory().get())
          .sslProvider(SslProvider.valueOf(tlsConfig.getSslProvider()));
      sslFactory.getTrustManagerFactory().ifPresent(sslContextBuilder::trustManager);

      if (tlsConfig.isClientAuthEnabled()) {
        sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
      }

      return GrpcSslContexts.configure(sslContextBuilder).build();
    }
  }

  @VisibleForTesting
  static GrpcConfig createQueryClientConfig(PinotConfiguration brokerConf) {
    Map<String, Object> target = brokerConf.toMap();
    target.put(GrpcConfig.CONFIG_USE_PLAIN_TEXT,
        !brokerConf.getProperty(CommonConstants.Broker.Grpc.KEY_OF_GRPC_TLS_ENABLED, false));
    Map<String, Object> convertedTlsMap =
        target.keySet()
            .stream()
            .filter(propName -> propName.startsWith(
                CommonConstants.Broker.Grpc.KEY_OF_GRPC_TLS_PREFIX))
            .collect(Collectors.toMap(
                propName -> GrpcConfig.GRPC_TLS_PREFIX + "." + propName.substring(
                    CommonConstants.Broker.Grpc.KEY_OF_GRPC_TLS_PREFIX.length() + 1),
                target::get
            ));
    target.putAll(convertedTlsMap);
    return new GrpcConfig(target);
  }

  public static boolean isEnabled(PinotConfiguration brokerConf) {
    return (brokerConf.getProperty(CommonConstants.Broker.Grpc.KEY_OF_GRPC_PORT, -1) > 0)
        || (brokerConf.getProperty(CommonConstants.Broker.Grpc.KEY_OF_GRPC_TLS_PORT, -1) > 0);
  }

  public static int getGrpcPort(PinotConfiguration brokerConf) {
    int secureGrpcPort = brokerConf.getProperty(CommonConstants.Broker.Grpc.KEY_OF_GRPC_TLS_PORT, -1);
    if (secureGrpcPort > 0) {
      return secureGrpcPort;
    }
    return brokerConf.getProperty(CommonConstants.Broker.Grpc.KEY_OF_GRPC_PORT, -1);
  }
}
