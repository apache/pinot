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

import com.linkedin.pinot.core.query.FilterQuery;
import com.linkedin.pinot.core.query.FilterQuery.FilterOperator;
import com.linkedin.pinot.query.request.AggregationInfo;
import com.linkedin.pinot.query.request.Query;


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

    Query query = new Query();
    query.setResourceName(res);

    query.setTableName(table);

    AggregationInfo inf = new AggregationInfo();
    inf.setAggregationType(function);

    Map<String, String> params = new HashMap<String, String>();
    params.put("column", metric);
    inf.setParams(params);

    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(inf);
    query.setAggregationsInfo(aggregationsInfo);
    
    FilterQuery filterQuery = null;
    if (filterColumn.contains(",")) {
      String []filterColumns = filterColumn.split(",");
      String []filterValues = filterVal.split(",");
      List<FilterQuery> nested = new ArrayList<FilterQuery>();
      for (int i = 0; i < filterColumns.length; i++) {
        FilterQuery d = new FilterQuery();
        d.setOperator(FilterOperator.EQUALITY);
        d.setColumn(filterColumns[i]);
        List<String> vals = new ArrayList<String>();
        vals.add(filterValues[i]);
        d.setValue(vals);
        nested.add(d);
      }
      filterQuery = new FilterQuery();
      filterQuery.setOperator(FilterOperator.AND);
      filterQuery.setNestedFilterQueries(nested);
    } else {
      filterQuery = new FilterQuery();
      filterQuery.setColumn(filterColumn);
      filterQuery.setOperator(FilterOperator.EQUALITY);
      List<String> vals = new ArrayList<String>();
      vals.add(filterVal);
      filterQuery.setValue(vals);
    }

    query.setFilterQuery(filterQuery);

    logger.info("*********************** isseing query : " + query);

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
