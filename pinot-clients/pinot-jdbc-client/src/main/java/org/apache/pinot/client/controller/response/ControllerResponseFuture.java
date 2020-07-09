package org.apache.pinot.client.controller.response;

import com.ning.http.client.Response;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.client.PinotClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


abstract class ControllerResponseFuture<T> implements Future<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerResponseFuture.class);
  private final Future<Response> _response;
  private final String _url;

  public ControllerResponseFuture(Future<Response> response, String url) {
    _response = response;
    _url = url;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return _response.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled() {
    return _response.isCancelled();
  }

  @Override
  public boolean isDone() {
    return _response.isDone();
  }

  @Override
  public T get()
      throws ExecutionException {
    return get(1000L, TimeUnit.DAYS);
  }

  abstract public T get(long timeout, TimeUnit unit)
      throws ExecutionException;

  public String getStringResponse(long timeout, TimeUnit unit)
      throws ExecutionException {
    try {
      LOGGER.debug("Sending request to {}", _url);

      Response httpResponse = _response.get(timeout, unit);

      LOGGER.debug("Completed request, HTTP status is {}", httpResponse.getStatusCode());

      if (httpResponse.getStatusCode() != 200) {
        throw new PinotClientException("Pinot returned HTTP status " + httpResponse.getStatusCode() + ", expected 200");
      }

      String responseBody = httpResponse.getResponseBody("UTF-8");

      return responseBody;
    } catch (Exception e) {
      throw new ExecutionException(e);
    }
  }
}
