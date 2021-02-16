export default function (server) {
  /**
   * get request for the entity mapping
   */
  server.get('/entityMapping/view/fromURN/:urn', () => {
    return [
      {
        id: 1234567,
        version: 1,
        createdBy: null,
        updatedBy: null,
        fromURN: 'thirdeye:metric:1',
        toURN: 'thirdeye:metric:100000'
      }
    ];
  });

  /**
   * get request to fetch all datasets
   */
  server.get('/data/datasets', () => {
    return ['dataset 1', 'dataset 2', 'dataset 3'];
  });

  /**
   * get request for rules
   */
  server.get('/detection/rule', () => {
    return [];
  });

  server.get('/external/services/all', () => {
    return ['service 1', 'service 2', 'service 3'];
  });
}
