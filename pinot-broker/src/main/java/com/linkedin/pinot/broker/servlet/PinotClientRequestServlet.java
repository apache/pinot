package com.linkedin.pinot.broker.servlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.antlr.runtime.RecognitionException;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.broker.broker.request.RequestConverter;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.pql.parsers.PQLCompiler;


public class PinotClientRequestServlet extends HttpServlet {
  private static final PQLCompiler requestCompiler = new PQLCompiler(new HashMap<String, String[]>());

  private static final long serialVersionUID = -3516093545255816357L;
  private static final Logger logger = LoggerFactory.getLogger(PinotClientRequestServlet.class);

  // private BrokerRequestHandler broker;
  @Override
  public void init(ServletConfig config) throws ServletException {
    // broker = (BrokerRequestHandler) config.getServletContext().getAttribute(BrokerRequestHandler.class.toString());
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    try {
      handleRequest(new JSONObject(req.getParameter("bql")));
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    try {
      handleRequest(extractJSON(req));
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  private void handleRequest(JSONObject request) throws Exception {
    String bql = request.getString("bql");
    JSONObject compiled = requestCompiler.compile(bql);
    BrokerRequest brokerRequest = convertToBrokerRequest(compiled);
  }

  private BrokerRequest convertToBrokerRequest(JSONObject compiled) throws Exception {
    return RequestConverter.fromJSON(compiled);
  }

  private JSONObject extractJSON(HttpServletRequest req) throws IOException, JSONException {
    StringBuffer requestStr = new StringBuffer();
    String line = null;
    BufferedReader reader = req.getReader();
    while ((line = reader.readLine()) != null)
      requestStr.append(line);
    return new JSONObject(requestStr.toString());
  }
}
