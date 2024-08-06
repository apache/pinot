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
package org.apache.pinot.plugin.inputformat.avro.datahub.deserializer;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a simple http util with connection pool to send get and post request
 */
public class HttpUtil {

    private HttpUtil() {
        // Utility Class
    }
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtil.class);
    private static PoolingHttpClientConnectionManager _poolConnManager = null;
    private static CloseableHttpClient _httpClient;

    static {
        try {
            LOGGER.info("start initial http client......");
            SSLContextBuilder builder = new SSLContextBuilder();
            builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build());
            // both support HTTP and HTTPS
            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", PlainConnectionSocketFactory.getSocketFactory())
                    .register("https", sslsf).build();
            _poolConnManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
            // max connection numbers
            _poolConnManager.setMaxTotal(640);
            _poolConnManager.setDefaultMaxPerRoute(320);
            _httpClient = getConnection();
            LOGGER.info(" initial http client successfully......");
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            LOGGER.error("Could not create httpClient object!!");
        }
    }

    private static CloseableHttpClient getConnection() {
        RequestConfig config = RequestConfig.custom().setConnectTimeout(5000)
                .setConnectionRequestTimeout(5000).setSocketTimeout(5000).build();
        return HttpClients.custom()
                // set connection pool
                .setConnectionManager(_poolConnManager)
                .setDefaultRequestConfig(config)
                // set retry times
                .setRetryHandler(new DefaultHttpRequestRetryHandler(2, false)).build();
    }

    public static String post(String uri, AbstractHttpEntity entity, Map<String, String> header) {
        HttpPost httpPost = new HttpPost(uri);
        CloseableHttpResponse response = null;
        try {
            httpPost.setEntity(entity);
            if (header != null && !header.isEmpty()) {
                header.forEach(httpPost::addHeader);
            }
            response = _httpClient.execute(httpPost);
            int code = response.getStatusLine().getStatusCode();
            String result = EntityUtils.toString(response.getEntity());
            if (code == HttpStatus.SC_OK) {
                return result;
            } else {
                LOGGER.error("request {} error:{}, body:{}, {}", uri, code, EntityUtils.toString(entity), result);
                return null;
            }
        } catch (IOException e) {
            LOGGER.error("http post exception ", e);
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                LOGGER.error("close exception ", e);
            }
        }
        return null;
    }

    public static String get(String uri, Map<String, String> header) {
        HttpGet httpGet = new HttpGet(uri);
        CloseableHttpResponse response = null;
        try {
            if (header != null && !header.isEmpty()) {
                header.forEach(httpGet::addHeader);
            }
            response = _httpClient.execute(httpGet);
            int code = response.getStatusLine().getStatusCode();
            String result = EntityUtils.toString(response.getEntity());
            if (code == HttpStatus.SC_OK) {
                return result;
            } else {
                LOGGER.error("request {} error:{}, {}", uri, code, result);
                return null;
            }
        } catch (IOException e) {
            LOGGER.error("http get exception ", e);
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                LOGGER.error("got io exception", e);
            }
        }
        return null;
    }
}
