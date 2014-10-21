package com.linkedin.pinot.core.query.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.data.manager.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.ResourceDataManager;
import com.linkedin.pinot.core.data.manager.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV2;
import com.linkedin.pinot.core.plan.maker.PlanMaker;
import com.linkedin.pinot.core.query.config.QueryExecutorConfig;
import com.linkedin.pinot.core.query.pruner.SegmentPrunerService;
import com.linkedin.pinot.core.query.pruner.SegmentPrunerServiceImpl;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;


public class ServerQueryExecutorV1Impl implements QueryExecutor {

  private static Logger LOGGER = LoggerFactory.getLogger(ServerQueryExecutorV1Impl.class);

  private static final String Domain = "com.linkedin.pinot";
  private QueryExecutorConfig _queryExecutorConfig = null;
  private InstanceDataManager _instanceDataManager = null;
  private SegmentPrunerService _segmentPrunerService = null;
  private PlanMaker _planMaker = null;
  private Timer _queryExecutorTimer = null;
  private boolean _isStarted = false;
  private long _timeOutMs = 15000;
  private boolean _printQueryPlan = true;

  public ServerQueryExecutorV1Impl() {
  }

  public ServerQueryExecutorV1Impl(boolean printQueryPlan) {
    _printQueryPlan = printQueryPlan;
  }

  @Override
  public void init(Configuration queryExecutorConfig, DataManager dataManager) throws ConfigurationException {
    _queryExecutorConfig = new QueryExecutorConfig(queryExecutorConfig);
    _instanceDataManager = (InstanceDataManager) dataManager;
    if (_queryExecutorConfig.getTimeOut() > 0) {
      _timeOutMs = _queryExecutorConfig.getTimeOut();
    }
    LOGGER.info("Timeout for query executor : " + _timeOutMs);
    LOGGER.info("Trying to build SegmentPrunerService");
    if (_segmentPrunerService == null) {
      _segmentPrunerService = new SegmentPrunerServiceImpl(_queryExecutorConfig.getPrunerConfig());
    }
    LOGGER.info("Trying to build QueryPlanMaker");
    _planMaker = new InstancePlanMakerImplV2(_timeOutMs);
    LOGGER.info("Trying to build QueryExecutorTimer");
    if (_queryExecutorTimer == null) {
      _queryExecutorTimer =
          Metrics.newTimer(new MetricName(Domain, "timer", "query-executor-time-"), TimeUnit.MILLISECONDS,
              TimeUnit.SECONDS);
    }
  }

  @Override
  public DataTable processQuery(InstanceRequest instanceRequest) {
    long start = System.currentTimeMillis();
    final BrokerRequest brokerRequest = instanceRequest.getQuery();

    LOGGER.info("Incoming query is :" + brokerRequest);
    List<IndexSegment> queryableSegmentDataManagerList = getPrunedQueryableSegments(instanceRequest);
    LOGGER.info("Matched " + queryableSegmentDataManagerList.size() + " segments! ");
    if (queryableSegmentDataManagerList.isEmpty()) {
      return new DataTable();
    }
    final Plan globalQueryPlan =
        _planMaker.makeInterSegmentPlan(queryableSegmentDataManagerList, brokerRequest, _instanceDataManager
            .getResourceDataManager(brokerRequest.getQuerySource().getResourceName()).getExecutorService());
    if (_printQueryPlan) {
      System.out.println("*********************************** query plan ***********************************");
      globalQueryPlan.print();
      System.out.println("*********************************** end query plan ***********************************");
    }
    globalQueryPlan.execute();
    DataTable instanceResponse = globalQueryPlan.getInstanceResponse();
    long end = System.currentTimeMillis();
    LOGGER.info("searching instance, browse took: " + (end - start));

    instanceResponse.getMetadata().put("timeUsedMs", Long.toString((end - start)));
    return instanceResponse;
  }

  private List<IndexSegment> getPrunedQueryableSegments(InstanceRequest instanceRequest) {
    String resourceName = instanceRequest.getQuery().getQuerySource().getResourceName();
    ResourceDataManager resourceDataManager = _instanceDataManager.getResourceDataManager(resourceName);
    if (resourceDataManager == null) {
      return null;
    }
    List<IndexSegment> queryableSegmentDataManagerList = new ArrayList<IndexSegment>();
    for (SegmentDataManager segmentDataManager : resourceDataManager.getAllSegments()) {
      IndexSegment indexSegment = segmentDataManager.getSegment();
      if (!_segmentPrunerService.prune(indexSegment, instanceRequest.getQuery())) {
        queryableSegmentDataManagerList.add(indexSegment);
      }
    }
    return queryableSegmentDataManagerList;
  }

  @Override
  public synchronized void shutDown() {
    if (isStarted()) {
      _isStarted = false;
      LOGGER.info("QueryExecutor is shutDown!");
    } else {
      LOGGER.warn("QueryExecutor is already shutDown, won't do anything!");
    }
  }

  public boolean isStarted() {
    return _isStarted;
  }

  @Override
  public synchronized void start() {
    _isStarted = true;
    LOGGER.info("QueryExecutor is started!");
  }
}
