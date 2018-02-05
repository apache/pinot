export default function(server) {
  /**
   * Get request for rootcause page
   * @return {Array}
   */
  server.get(`/rootcause/raw`, () => {
    return [ {
      "urn" : "thirdeye:metric:1",
      "score" : 1.0,
      "label" : "thirdeye::pageViews",
      "type" : "metric",
      "link" : null,
      "relatedEntities" : [ ],
      "attributes" : {
        "inverse" : [ "false" ],
        "dataset" : [ "thirdeye" ],
        "derived" : [ "false" ],
        "additive" : [ "true" ]
      }
    } ];
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
      "id": 1,
      "alias": "pageViews"
    };
  });

  /**
   * Retrieves timestamp of last element in timeseries
   */
  server.get('/data/maxDataTime/metricId/1', () => {
    return 1517846399998;
  });
}
