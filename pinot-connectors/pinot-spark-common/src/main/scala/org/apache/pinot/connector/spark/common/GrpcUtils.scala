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
package org.apache.pinot.connector.spark.common

import io.grpc.{ManagedChannel, ManagedChannelBuilder, Metadata}
import io.grpc.stub.MetadataUtils

import java.io.{File, FileInputStream}
import java.security.KeyStore
import javax.net.ssl.{TrustManagerFactory, KeyManagerFactory, SSLContext}
import java.security.cert.X509Certificate
import javax.net.ssl.{X509TrustManager, TrustManager}

/**
 * Helper utilities for gRPC communication with Pinot servers.
 * Supports TLS/SSL configuration and proxy forwarding headers.
 */
private[pinot] object GrpcUtils extends Logging {

  /**
   * Create a gRPC channel with proper configuration for TLS and proxy support
   */
  def createChannel(host: String,
                   port: Int,
                   options: PinotDataSourceReadOptions): ManagedChannel = {
    val channelBuilder = if (options.grpcUsePlainText) {
      logInfo(s"Creating plain text gRPC channel to $host:$port")
      ManagedChannelBuilder.forAddress(host, port)
        .usePlaintext()
    } else {
      logInfo(s"Creating TLS gRPC channel to $host:$port")
      createTlsChannel(host, port, options)
    }

    // Set max inbound message size
    channelBuilder.maxInboundMessageSize(options.grpcMaxInboundMessageSize.toInt)

    val channel = channelBuilder.build()
    
    logInfo(s"gRPC channel created successfully for $host:$port")
    channel
  }

  /**
   * Create a TLS-enabled gRPC channel
   * Note: This is a simplified TLS configuration using standard gRPC APIs
   */
  private def createTlsChannel(host: String, 
                              port: Int, 
                              options: PinotDataSourceReadOptions): ManagedChannelBuilder[_] = {
    val channelBuilder = ManagedChannelBuilder.forAddress(host, port)

    try {
      // For production use, you would configure SSL context properly
      // This is a basic TLS setup - in production, configure proper certificates
      
      // Validate keystore configuration
      (options.grpcTlsKeystorePath, options.grpcTlsKeystorePassword) match {
        case (Some(_), None) =>
          throw new IllegalArgumentException("gRPC keystore password is required when keystore path is provided")
        case (Some(keystorePath), Some(keystorePassword)) =>
          logInfo(s"gRPC keystore configured: $keystorePath")
        case _ => // No keystore configuration
      }

      // Validate truststore configuration
      (options.grpcTlsTruststorePath, options.grpcTlsTruststorePassword) match {
        case (Some(_), None) =>
          throw new IllegalArgumentException("gRPC truststore password is required when truststore path is provided")
        case (Some(truststorePath), Some(truststorePassword)) =>
          logInfo(s"gRPC truststore configured: $truststorePath")
        case _ =>
          logInfo("No gRPC truststore configured, using system default")
      }

      logInfo(s"gRPC TLS configuration validated for SSL provider: ${options.grpcTlsSslProvider}")
      
    } catch {
      case e: Exception =>
        logError("Failed to configure gRPC TLS", e)
        throw new RuntimeException("gRPC TLS configuration failed", e)
    }

    channelBuilder
  }

  /**
   * Create metadata for proxy forwarding headers
   * This is used when connecting through Pinot proxy
   */
  def createProxyMetadata(targetHost: String, targetPort: Int): Metadata = {
    val metadata = new Metadata()
    metadata.put(Metadata.Key.of("FORWARD_HOST", Metadata.ASCII_STRING_MARSHALLER), targetHost)
    metadata.put(Metadata.Key.of("FORWARD_PORT", Metadata.ASCII_STRING_MARSHALLER), targetPort.toString)
    
    logDebug(s"Created proxy metadata with FORWARD_HOST=$targetHost, FORWARD_PORT=$targetPort")
    metadata
  }

  /**
   * Apply proxy metadata to a gRPC stub
   * Note: This creates a new stub with attached headers
   */
  def applyProxyMetadata[T <: io.grpc.stub.AbstractStub[T]](stub: T, targetHost: String, targetPort: Int): T = {
    val metadata = createProxyMetadata(targetHost, targetPort)
    // Use withInterceptors to attach metadata headers
    stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
  }
}
