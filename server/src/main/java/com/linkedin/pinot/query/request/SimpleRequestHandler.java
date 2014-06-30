package com.linkedin.pinot.query.request;

import io.netty.buffer.ByteBuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.linkedin.pinot.query.executor.QueryExecutor;
import com.linkedin.pinot.query.response.Error;
import com.linkedin.pinot.query.response.InstanceResponse;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandler;


/**
 * A simple implementation of RequestHandler.
 * 
 * @author xiafu
 *
 */
public class SimpleRequestHandler implements RequestHandler {

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
      Request queryRequest = (Request) is.readObject();

      instanceResponse = _queryExecutor.processQuery(queryRequest);
    } catch (IOException e1) {
      return null;
    } catch (Exception e) {
      instanceResponse = new InstanceResponse();
      Error error = new Error();
      error.setError(400, "Internal Query Process Error.\n" + e.getMessage());
      instanceResponse.setError(error);
    }
    try {
      ObjectOutputStream os = new ObjectOutputStream(out);
      os.writeObject(instanceResponse);
    } catch (IOException e) {
      return null;
    }
    return out.toByteArray();
  }

}
