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

import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.client5.http.impl.classic.{HttpClientBuilder, HttpClients}
import org.apache.hc.client5.http.ssl.{SSLConnectionSocketFactory, TrustAllStrategy}
import org.apache.hc.core5.http.io.entity.EntityUtils
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder
import org.apache.hc.core5.http.ClassicHttpRequest
import org.apache.hc.core5.ssl.{SSLContextBuilder, SSLContexts}
import org.apache.hc.core5.util.Timeout

import java.io.{File, FileInputStream}
import java.net.URI
import java.security.KeyStore
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLContext

/**
 * Helper Http methods to get metadata information from Pinot controller/broker.
 * Supports both HTTP and HTTPS with SSL/TLS configuration.
 */
private[pinot] object HttpUtils extends Logging {
  private val GET_REQUEST_SOCKET_TIMEOUT_MS = 5 * 1000 // 5 mins
  private val GET_REQUEST_CONNECT_TIMEOUT_MS = 10 * 1000 // 10 mins

  private val requestConfig = RequestConfig
      .custom()
      .setConnectTimeout(Timeout.of(GET_REQUEST_CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS))
      .setResponseTimeout(Timeout.of(GET_REQUEST_SOCKET_TIMEOUT_MS, TimeUnit.MILLISECONDS))
      .build()

  // Thread-safe clients for HTTP and HTTPS
  private val httpClient = HttpClients.custom()
    .setDefaultRequestConfig(requestConfig)
    .build()

  private var httpsClient: Option[org.apache.hc.client5.http.impl.classic.CloseableHttpClient] = None

  /**
   * Configure HTTPS client with SSL/TLS settings
   */
  def configureHttpsClient(keystorePath: Option[String],
                          keystorePassword: Option[String],
                          truststorePath: Option[String],
                          truststorePassword: Option[String]): Unit = {
    try {
      val sslContextBuilder = SSLContexts.custom()

      // Configure keystore if provided
      (keystorePath, keystorePassword) match {
        case (Some(ksPath), Some(ksPassword)) =>
          val keystore = KeyStore.getInstance(KeyStore.getDefaultType)
          keystore.load(new FileInputStream(new File(ksPath)), ksPassword.toCharArray)
          sslContextBuilder.loadKeyMaterial(keystore, ksPassword.toCharArray)
          logInfo(s"Configured keystore: $ksPath")
        case (Some(_), None) =>
          throw new IllegalArgumentException("Keystore password is required when keystore path is provided")
        case _ => // No keystore configuration
      }

      // Configure truststore if provided, otherwise trust all certificates
      (truststorePath, truststorePassword) match {
        case (Some(tsPath), Some(tsPassword)) =>
          val truststore = KeyStore.getInstance(KeyStore.getDefaultType)
          truststore.load(new FileInputStream(new File(tsPath)), tsPassword.toCharArray)
          sslContextBuilder.loadTrustMaterial(truststore, null)
          logInfo(s"Configured truststore: $tsPath")
        case (Some(_), None) =>
          throw new IllegalArgumentException("Truststore password is required when truststore path is provided")
        case _ =>
          // If no truststore is provided, trust all certificates (not recommended for production)
          sslContextBuilder.loadTrustMaterial(null, TrustAllStrategy.INSTANCE)
          logWarning("No truststore configured, trusting all certificates (not recommended for production)")
      }

      val sslContext = sslContextBuilder.build()
      val sslSocketFactory = new SSLConnectionSocketFactory(sslContext)

      // Create a connection manager with SSL socket factory
      val connectionManager = org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder.create()
        .setSSLSocketFactory(sslSocketFactory)
        .build()

      httpsClient = Some(HttpClients.custom()
        .setDefaultRequestConfig(requestConfig)
        .setConnectionManager(connectionManager)
        .build())
      
      logInfo("HTTPS client configured successfully")
    } catch {
      case e: Exception =>
        logError("Failed to configure HTTPS client", e)
        throw new RuntimeException("HTTPS configuration failed", e)
    }
  }

  def sendGetRequest(uri: URI): String = {
    sendGetRequest(uri, None, None)
  }

  def sendGetRequest(uri: URI, authHeader: Option[String], authToken: Option[String]): String = {
    val requestBuilder = ClassicRequestBuilder.get(uri)
    
    // Add authentication header if provided
    AuthUtils.buildAuthHeader(authHeader, authToken).foreach { case (name, value) =>
      requestBuilder.addHeader(name, value)
    }
    
    executeRequest(requestBuilder.build(), uri.getScheme.toLowerCase == "https")
  }

  /**
   * Send GET request with proxy headers for forwarding to the actual Pinot service
   * This method adds FORWARD_HOST and FORWARD_PORT headers as required by Pinot proxy
   */
  def sendGetRequestWithProxyHeaders(uri: URI, 
                                    authHeader: Option[String], 
                                    authToken: Option[String],
                                    serviceType: String,
                                    targetHost: String): String = {
    val requestBuilder = ClassicRequestBuilder.get(uri)
    
    // Add authentication header if provided
    AuthUtils.buildAuthHeader(authHeader, authToken).foreach { case (name, value) =>
      requestBuilder.addHeader(name, value)
    }
    
    // No need to forward controller requests
    if (!serviceType.equalsIgnoreCase("controller")) {
      // Add proxy forwarding headers as used by Pinot proxy
      // Parse host and port from targetHost (format: host:port or just host)
      val (host, effectivePort) = targetHost.split(":", 2) match {
        case Array(h, p) if p.forall(_.isDigit) =>
          (h, p)
        case Array(h) =>
          val defaultPort = if (uri.getScheme.equalsIgnoreCase("https")) "443" else "80"
          (h, defaultPort)
        case _ =>
          throw new IllegalArgumentException(s"Invalid targetHost: $targetHost")
      }
      requestBuilder.addHeader("FORWARD_HOST", host)
      requestBuilder.addHeader("FORWARD_PORT", effectivePort)
      logDebug(s"Sending proxy request to $uri with forward headers: host=$host, port=$effectivePort")
    }

    executeRequest(requestBuilder.build(), uri.getScheme.toLowerCase == "https")
  }

  private def executeRequest(httpRequest: ClassicHttpRequest, useHttps: Boolean = false): String = {
    val client = if (useHttps) {
      httpsClient.getOrElse {
        throw new IllegalStateException("HTTPS client not configured. Call configureHttpsClient() first.")
      }
    } else {
      httpClient
    }
    
    val response = client.execute(httpRequest)
    try {
      val statusCode = response.getCode
      if (statusCode >= 200 && statusCode < 300) {
        if (response.getEntity != null) {
          EntityUtils.toString(response.getEntity, "UTF-8")
        } else {
          throw new IllegalStateException("Http response content is empty!?")
        }
      } else {
        throw HttpStatusCodeException(
          s"Got error status code '$statusCode' with reason '${response.getReasonPhrase}'",
          statusCode
        )
      }
    } finally {
      response.close()
    }
  }

  def close(): Unit = {
    httpClient.close()
    httpsClient.foreach(_.close())
  }
}
