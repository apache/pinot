package com.linkedin.pinot.server.starter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.QuerySource;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;


public class SimpleQueryRequestServlet extends HttpServlet {
  private static final Logger logger = Logger.getLogger(SimpleQueryRequestServlet.class);

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  InteractiveBroker broker = null;

  @Override
  public void init(ServletConfig config) throws ServletException {
    broker = (InteractiveBroker) config.getServletContext().getAttribute(InteractiveBroker.class.toString());
    logger.info("************************************ broker : " + broker);
  }

  @Override
  protected synchronized void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
      IOException {

    String function = req.getParameter("function");
    String metric = req.getParameter("metric");
    String filterColumn = req.getParameter("filterColumn");
    String filterVal = req.getParameter("filterVal");
    String res = req.getParameter("resource");
    String table = req.getParameter("table");

    logger.info("found all params as : " + req.getParameterMap());

    BrokerRequest brokerRequest = new BrokerRequest();
    QuerySource querySource = new QuerySource();
    querySource.setResourceName(res);
    querySource.setTableName(table);
    brokerRequest.setQuerySource(querySource);

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(function);
    Map<String, String> aggregationParams = new HashMap<String, String>();
    aggregationParams.put("column", metric);
    aggregationInfo.setAggregationParams(aggregationParams);

    List<AggregationInfo> aggregationInfos = new ArrayList<AggregationInfo>();
    aggregationInfos.add(aggregationInfo);

    brokerRequest.setAggregationsInfo(aggregationInfos);

    FilterQueryTree filterQueryTree;
    if (filterColumn.contains(",")) {
      String[] filterColumns = filterColumn.split(",");
      String[] filterValues = filterVal.split(",");
      List<FilterQueryTree> nested = new ArrayList<FilterQueryTree>();
      for (int i = 0; i < filterColumns.length; i++) {

        List<String> vals = new ArrayList<String>();
        vals.add(filterValues[i]);
        FilterQueryTree d = new FilterQueryTree(i + 1, filterColumns[i], vals, FilterOperator.EQUALITY, null);
        nested.add(d);
      }
      filterQueryTree = new FilterQueryTree(0, null, null, FilterOperator.AND, nested);
    } else {
      List<String> vals = new ArrayList<String>();
      vals.add(filterVal);
      filterQueryTree = new FilterQueryTree(0, filterColumn, vals, FilterOperator.EQUALITY, null);
    }

    RequestUtils.generateFilterFromTree(filterQueryTree, brokerRequest);

    logger.info("*********************** issueing query : " + brokerRequest);

    String response = "NOT_WORKING_RIGHT_NOW MAKE CHANGES TO THE SERVLET";
    try {
      //response = broker.issueQuery();
    } catch (Exception e) {
      response = e.getMessage();
      e.printStackTrace();
    }

    logger.info("************************************** came back with : " + response);

    resp.getOutputStream().print(response);
    resp.getOutputStream().flush();
    resp.getOutputStream().close();
    resp.setStatus(200);
    return;
  }
}
