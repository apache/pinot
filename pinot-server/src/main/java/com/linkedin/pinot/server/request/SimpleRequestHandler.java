package com.linkedin.pinot.server.request;

import io.netty.buffer.ByteBuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.response.InstanceResponse;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.DataTableBuilder;
import com.linkedin.pinot.serde.SerDe;
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

    System.out.println("processing request : " + request);

    DataTable instanceResponse = null;
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    byte[] byteArray = new byte[request.readableBytes()];
    request.readBytes(byteArray);
    SerDe serDe = new SerDe(new TCompactProtocol.Factory());
    InstanceRequest queryRequest = null;
    try {

      queryRequest = new InstanceRequest();
      serDe.deserialize(queryRequest, byteArray);

      System.out.println("instance request : " + queryRequest);

      instanceResponse = _queryExecutor.processQuery(queryRequest);

      System.out.println("******************************");
      System.out.println(instanceResponse);
    } catch (Exception e) {
      LOGGER.error("Got exception while processing request. Returning error response", e);
      DataTableBuilder dataTableBuilder = new DataTableBuilder(null);
      List<ProcessingException> exceptions = new ArrayList<ProcessingException>();
      exceptions.add(new ProcessingException(250));
      instanceResponse = dataTableBuilder.buildExceptions();
    }
    try {
      if (instanceResponse == null) {
        return new byte[0];
      } else {
        return instanceResponse.toBytes();
      }
    } catch (Exception e) {
      LOGGER.error("Got exception while serializing response.", e);
      return null;
    }
  }

}
