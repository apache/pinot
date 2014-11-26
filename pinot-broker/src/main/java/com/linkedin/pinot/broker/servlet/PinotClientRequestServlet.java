package com.linkedin.pinot.broker.servlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.pql.parsers.PQLCompiler;
import com.linkedin.pinot.requestHandler.BrokerRequestHandler;
import com.linkedin.pinot.transport.common.BucketingSelection;
import com.linkedin.pinot.transport.common.SegmentId;


public class PinotClientRequestServlet extends HttpServlet {
  private static final PQLCompiler requestCompiler = new PQLCompiler(new HashMap<String, String[]>());

  private static final long serialVersionUID = -3516093545255816357L;
  private static final Logger logger = LoggerFactory.getLogger(PinotClientRequestServlet.class);

  private BrokerRequestHandler broker;

  @Override
  public void init(ServletConfig config) throws ServletException {
    broker = (BrokerRequestHandler) config.getServletContext().getAttribute(BrokerRequestHandler.class.toString());
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    try {
      resp.getOutputStream().print(handleRequest(new JSONObject(req.getParameter("bql"))).toJson().toString());
      resp.getOutputStream().flush();
      resp.getOutputStream().close();
    } catch (final Exception e) {
      resp.getOutputStream().print(e.getMessage());
      resp.getOutputStream().flush();
      resp.getOutputStream().close();
      logger.error(e.getMessage());
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    try {
      resp.getOutputStream().print(handleRequest(extractJSON(req)).toJson().toString());
      resp.getOutputStream().flush();
      resp.getOutputStream().close();
    } catch (final Exception e) {
      resp.getOutputStream().print(e.getMessage());
      resp.getOutputStream().flush();
      resp.getOutputStream().close();
      logger.error(e.getMessage());
      System.out.println(e);
      e.printStackTrace();
    }
  }

  private BrokerResponse handleRequest(JSONObject request) throws Exception {
    final String bql = request.getString("pql");
    System.out.println(bql);
    final JSONObject compiled = requestCompiler.compile(bql);
    final BrokerRequest brokerRequest = convertToBrokerRequest(compiled);
    System.out.println(brokerRequest);
    System.out.println(brokerRequest.getQuerySource().getResourceName());
    final BucketingSelection bucketingSelection = getBucketingSelection(brokerRequest);
    final BrokerResponse resp = (BrokerResponse) broker.processBrokerRequest(brokerRequest, bucketingSelection);
    logger.info("Broker Response : " + resp);
    return resp;
  }

  private BucketingSelection getBucketingSelection(BrokerRequest brokerRequest) {
    final Map<SegmentId, ServerInstance> bucketMap = new HashMap<SegmentId, ServerInstance>();
    return new BucketingSelection(bucketMap);
  }

  private BrokerRequest convertToBrokerRequest(JSONObject compiled) throws Exception {
    return com.linkedin.pinot.common.client.request.RequestConverter.fromJSON(compiled);
  }

  private JSONObject extractJSON(HttpServletRequest req) throws IOException, JSONException {
    final JSONObject ret = new JSONObject();
    final StringBuffer requestStr = new StringBuffer();
    String line = null;
    final BufferedReader reader = req.getReader();
    while ((line = reader.readLine()) != null) {
      requestStr.append(line);
    }
    return new JSONObject(requestStr.toString());
  }
}
