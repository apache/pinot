package com.linkedin.pinot.server.request;

import io.netty.buffer.ByteBuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.response.InstanceResponse;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandler;


/**
 * A simple implementation of RequestHandler.
 * 
 * @author xiafu
 *
 */
public class SimpleRequestHandler implements RequestHandler {

  private static Logger LOGGER = LoggerFactory.getLogger(SimpleRequestHandler.class);

  QueryExecutor _queryExecutor = null;

  public SimpleRequestHandler(QueryExecutor queryExecutor) {
    _queryExecutor = queryExecutor;
  }

  @Override
  public byte[] processRequest(ByteBuf request) {

    InstanceResponse instanceResponse = null;
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    byte[] byteArray = new byte[request.readableBytes()];
    request.readBytes(byteArray);
    ByteArrayInputStream in = new ByteArrayInputStream(byteArray);
    ObjectInputStream is;
    try {
      is = new ObjectInputStream(in);
      InstanceRequest queryRequest = (InstanceRequest) is.readObject();

      instanceResponse = _queryExecutor.processQuery(queryRequest);
    } catch (IOException e1) {
      return null;
    } catch (Exception e) {
      LOGGER.error("Got exception while processing request. Returning error response", e);
      instanceResponse = new InstanceResponse();
      List<ProcessingException> exceptions = new ArrayList<ProcessingException>();
      exceptions.add(new ProcessingException(250));
      instanceResponse.setExceptions(exceptions);
    }
    try {
      ObjectOutputStream os = new ObjectOutputStream(out);
      os.writeObject(instanceResponse);
    } catch (IOException e) {
      LOGGER.error("Got exception while serializing response.", e);
      return null;
    }
    return out.toByteArray();
  }

}
