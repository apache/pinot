

export default function (server) {
  /**
   * get request for the entity mapping
   */
  server.get('/entityMapping/view/fromURN/:urn', () => {
    return [
      { id: 1234567,
        version: 1,
        createdBy: null,
        updatedBy: null,
        fromURN: 'thirdeye:metric:1',
        toUrn: 'thirdeye:metric:100000'
      }
    ];
  });
}
