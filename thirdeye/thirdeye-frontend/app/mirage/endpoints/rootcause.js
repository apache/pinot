export default function(server) {
  /**
   * Get request for rootcause page
   */
  server.get(`/rootcause/raw`, () => {
    return [ {
      "urn" : "thirdeye:metric:1",
      "score" : 1.0,
      "label" : "thirdeyeKbmi::pageViews",
      "type" : "metric",
      "link" : null,
      "relatedEntities" : [ ],
      "attributes" : {
        "inverse" : [ "false" ],
        "dataset" : [ "thirdeyeKbmi" ],
        "derived" : [ "false" ],
        "additive" : [ "true" ]
      }
    } ];
  });

  server.get('/data/autocomplete/filters/metric/1', () => {
    return {
      environment: ['prod']
    };
  });

  server.get('/data/metric/1', () => {
    return {
      "id": 1,
      "name": "pageViews"
    };
  });

  server.get('/data/maxDataTime/metricId/1', () => {
    return 1;
  });
}
