export default function(server) {
  /**
   * Get request for rootcause page
   * @return {Array}
   */
  server.get(`/rootcause/raw`, () => {
    return [ {
      urn : "thirdeye:metric:1",
      score : 1.0,
      label : "thirdeye::pageViews",
      type : "metric",
      link : null,
      relatedEntities : [ ],
      attributes : {
        inverse : [ "false" ],
        dataset : [ "thirdeye" ],
        derived : [ "false" ],
        additive : [ "true" ],
        maxTime: [ 1517846400000 ],
        granularity: [ "1_HOURS" ]
      }
    }, {
      urn : "thirdeye:timerange:anomaly:1512400800000:1512428100000",
      score : 1.0,
      label : "thirdeye:timerange:anomaly:1512400800000:1512428100000",
      type : "other",
      link : null,
      relatedEntities : [ ],
      attributes : { }
    }, {
      urn : "thirdeye:timerange:analysis:1511423999000:1512028799000",
      score : 1.0,
      label : "thirdeye:timerange:analysis:1511423999000:1512028799000",
      type : "other",
      link : null,
      relatedEntities : [ ],
      attributes : { }
    }, {
      urn : "thirdeye:event:anomaly:1",
      score : 1.0,
      label : "anomaly_label",
      type : "event",
      link : "#/rootcause?anomalyId=1",
      relatedEntities : [ ],
      attributes : {
        function : [ "anomaly_label" ],
        status : [ "NOT_ANOMALY" ]
      }
    }];
  });

  /**
   * Retrieves the options for autocomplete for 'filter' dropdown
   */
  server.get('/data/autocomplete/filters/metric/1', () => {
    return {
      environment: ['prod']
    };
  });

  /**
   * Retrieves information about a metric
   */
  server.get('/data/metric/1', () => {
    return {
      id: 1,
      alias: "pageViews"
    };
  });

  /**
   * Retrieves information about a session
   */
  server.get('/session/1', () => {
    return {
      id : 1,
      version : 1,
      createdBy : "rootcauseuser",
      updatedBy : "rootcauseuser",
      name : "My Session",
      text : "Cause of anomaly is unknown",
      owner : "rootcauseuser",
      compareMode : "WoW",
      granularity : "1_HOURS",
      previousId : null,
      anomalyRangeStart : 1512400800000,
      anomalyRangeEnd : 1512428100000,
      analysisRangeStart : 1511856000000,
      analysisRangeEnd : 1512460799999,
      created : 1517363257776,
      updated : 1517363257776
    };
  });

  /**
   * Post request for saving anomaly reports
   */
  server.post(`/session`, () => {
    const hardCodedId = 1;
    return hardCodedId;
  });

  /**
   * TODO: Once API is finalized, have this call return something meaningful
   */
  server.get('/timeseries/query', () => {
    return {};
  });

  /**
   * TODO: Once API is finalized, have this call return something meaningful
   */
  server.get('/rootcause/query', () => {
    return {};
  });

  /**
   * TODO: Once API is finalized, have this call return something meaningful
   */
  server.get('/aggregation/aggregate', () => {
    return {};
});

  /**
   * TODO: Once API is finalized, have this call return something meaningful
   */
  server.get('/aggregation/query', () => {
    return {};
});

  /**
   * TODO: Once API is finalized, have this call return something meaningful
   */
  server.get('/session/query', () => {
    return {};
  });
}
