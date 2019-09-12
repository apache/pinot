package org.apache.pinot.benchmark.common.utils;

import com.sun.istack.internal.Nullable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotClusterClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotClusterClient.class);

  public static final int GET_REQUEST_SOCKET_TIMEOUT_MS = 5 * 1000; // 5 seconds

  private static final String HTTP = "http";
  private static final String HTTPS = "https";
  private static final String TABLES_PATH = "/tables";
  private final CloseableHttpClient _httpClient;

  public PinotClusterClient() {
    this(null);
  }

  public PinotClusterClient(@Nullable SSLContext sslContext) {
    _httpClient = HttpClients.custom().setSSLContext(sslContext).build();
  }

  private static URI getURI(String scheme, String host, int port, String path)
      throws URISyntaxException {
    return new URI(scheme, null, host, port, path, null, null);
  }

  public static URI getListAllTablesHttpURI(String host, int port)
      throws URISyntaxException {
    return getURI(HTTP, host, port, TABLES_PATH);
  }

  public static URI getRetrieveTableConfigHttpURI(String host, int port, String rawTableName)
      throws URISyntaxException {
    return getURI(HTTP, host, port, TABLES_PATH + "/" + rawTableName);
  }

  public SimpleHttpResponse sendGetRequest(URI uri)
      throws IOException, HttpErrorStatusException {
    return sendRequest(constructGetRequest(uri));
  }

  private static HttpUriRequest constructGetRequest(URI uri) {
    RequestBuilder requestBuilder = RequestBuilder.get(uri).setVersion(HttpVersion.HTTP_1_1);
    setTimeout(requestBuilder, GET_REQUEST_SOCKET_TIMEOUT_MS);
    return requestBuilder.build();
  }

  private static void setTimeout(RequestBuilder requestBuilder, int socketTimeoutMs) {
    RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(socketTimeoutMs).build();
    requestBuilder.setConfig(requestConfig);
  }

  private SimpleHttpResponse sendRequest(HttpUriRequest request)
      throws IOException, HttpErrorStatusException {
    try (CloseableHttpResponse response = _httpClient.execute(request)) {
      String controllerHost = null;
      String controllerVersion = null;
      if (response.containsHeader(CommonConstants.Controller.HOST_HTTP_HEADER)) {
        controllerHost = response.getFirstHeader(CommonConstants.Controller.HOST_HTTP_HEADER).getValue();
        controllerVersion = response.getFirstHeader(CommonConstants.Controller.VERSION_HTTP_HEADER).getValue();
      }
      if (controllerHost != null) {
        LOGGER.info(String
            .format("Sending request: %s to controller: %s, version: %s", request.getURI(), controllerHost,
                controllerVersion));
      }
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode >= 300) {
        throw new HttpErrorStatusException(getErrorMessage(request, response), statusCode);
      }
      return new SimpleHttpResponse(statusCode, EntityUtils.toString(response.getEntity()));
    }
  }

  private static String getErrorMessage(HttpUriRequest request, CloseableHttpResponse response) {
    String controllerHost = null;
    String controllerVersion = null;
    if (response.containsHeader(CommonConstants.Controller.HOST_HTTP_HEADER)) {
      controllerHost = response.getFirstHeader(CommonConstants.Controller.HOST_HTTP_HEADER).getValue();
      controllerVersion = response.getFirstHeader(CommonConstants.Controller.VERSION_HTTP_HEADER).getValue();
    }
    StatusLine statusLine = response.getStatusLine();
    String reason;
    try {
      reason = JsonUtils.stringToJsonNode(EntityUtils.toString(response.getEntity())).get("error").asText();
    } catch (Exception e) {
      reason = "Failed to get reason";
    }
    String errorMessage = String.format("Got error status code: %d (%s) with reason: \"%s\" while sending request: %s",
        statusLine.getStatusCode(), statusLine.getReasonPhrase(), reason, request.getURI());
    if (controllerHost != null) {
      errorMessage =
          String.format("%s to controller: %s, version: %s", errorMessage, controllerHost, controllerVersion);
    }
    return errorMessage;
  }
}
