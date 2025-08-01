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
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.apache.hc.core5.http.io.entity.EntityUtils
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder
import org.apache.hc.core5.http.ClassicHttpRequest
import org.apache.hc.core5.util.Timeout

import java.net.URI
import java.util.concurrent.TimeUnit

/**
 * Helper Http methods to get metadata information from Pinot controller/broker.
 */
private[pinot] object HttpUtils extends Logging {
  private val GET_REQUEST_SOCKET_TIMEOUT_MS = 5 * 1000 // 5 mins
  private val GET_REQUEST_CONNECT_TIMEOUT_MS = 10 * 1000 // 10 mins

  private val requestConfig = RequestConfig
      .custom()
      .setConnectTimeout(Timeout.of(GET_REQUEST_CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS))
      .setResponseTimeout(Timeout.of(GET_REQUEST_SOCKET_TIMEOUT_MS, TimeUnit.MILLISECONDS))
      .build()

  private val httpClient = HttpClients.custom()
    .setDefaultRequestConfig(requestConfig)
    .build()

  def sendGetRequest(uri: URI, authorization: String = ""): String = {
    val requestBuilder = ClassicRequestBuilder.get(uri)

    if (authorization.nonEmpty) {
      requestBuilder.addHeader("Authorization", s"$authorization")
    }

    executeRequest(requestBuilder.build())
  }

  private def executeRequest(httpRequest: ClassicHttpRequest): String = {
    val response = httpClient.execute(httpRequest)
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
  }
}
