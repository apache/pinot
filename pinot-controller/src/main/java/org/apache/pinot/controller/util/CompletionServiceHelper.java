package org.apache.pinot.controller.util;

import com.google.common.collect.BiMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.pinot.common.http.MultiGetRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a helper class that can be used to make HttpGet (MultiGet) calls and get the responses back.
 * The responses are returned as a string.
 *
 * The helper also records number of failed responses so that the caller knows if any of the calls
 * failed to respond. The failed instance is logged for debugging. 
 */
public class CompletionServiceHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompletionServiceHelper.class);

  private final Executor _executor;
  private final HttpConnectionManager _httpConnectionManager;
  private final BiMap<String, String> _endpointsToServers;

  public CompletionServiceHelper(Executor executor, HttpConnectionManager httpConnectionManager,
                                 BiMap<String, String> endpointsToServers) {
    _executor = executor;
    _httpConnectionManager = httpConnectionManager;
    _endpointsToServers = endpointsToServers;
  }

  public CompletionServiceResponse doMultiGetRequest(List<String> serverURLs, String tableNameWithType, int timeoutMs) {
    CompletionServiceResponse completionServiceResponse = new CompletionServiceResponse();

    // TODO: use some service other than completion service so that we know which server encounters the error
    CompletionService<GetMethod> completionService =
        new MultiGetRequest(_executor, _httpConnectionManager).execute(serverURLs, timeoutMs);
    for (int i = 0; i < serverURLs.size(); i++) {
      GetMethod getMethod = null;
      try {
        getMethod = completionService.take().get();
        URI uri = getMethod.getURI();
        String instance = _endpointsToServers.get(uri.getHost() + ":" + uri.getPort());
        if (getMethod.getStatusCode() >= 300) {
          LOGGER.error("Server: {} returned error: {}", instance, getMethod.getStatusCode());
          completionServiceResponse._failedResponseCount++;
          continue;
        }
        completionServiceResponse._httpResponses.put(instance, getMethod.getResponseBodyAsString());
      } catch (Exception e) {
        // Ignore individual exceptions because the exception has been logged in MultiGetRequest
        // Log the number of failed servers after gathering all responses
      } finally {
        if (getMethod != null) {
          getMethod.releaseConnection();
        }
      }
    }

    int numServersResponded = completionServiceResponse._httpResponses.size();
    if (numServersResponded != serverURLs.size()) {
      LOGGER.warn("Finish reading information for table: {} with {}/{} server responses", tableNameWithType,
          numServersResponded, serverURLs);
    } else {
      LOGGER.info("Finish reading information for table: {}", tableNameWithType);
    }
    return completionServiceResponse;
  }

  static public class CompletionServiceResponse {
    public Map<String, String> _httpResponses;
    public int _failedResponseCount;

    public CompletionServiceResponse() {
      _httpResponses = new HashMap<>();
      _failedResponseCount = 0;
    }
  }
}
