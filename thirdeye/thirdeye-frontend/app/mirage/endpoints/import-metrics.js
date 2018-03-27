export default function (server) {
  /**
   * get request for dashboard name verification
   */
  server.get('autometrics/isIngraphDashboard/:name', (schema, request) => {
    return request.params.name === 'thirdeye-all' ? true : false;
  });

  /**
   * Post request for creating a new dataset config in TE
   */
  server.post(`/onboard/create`, () => {
    return 'Created config with id 28863147';
  });

  /**
   * Post request for auto-onboarding newly imported metrics
   */
  server.post(`/autoOnboard/runAdhoc/AutometricsThirdeyeDataSource`, () => {
    return {};
  });

  /**
   * get request for metric import verification
   */
  server.get('/thirdeye-admin/metric-config/metrics', () => {
    return {
      'Result':'OK',
      'Records':[
      'thirdeye-all::thirdeye_rec1',
      'thirdeye-all::Thirdeye_rec2',
      'thirdeye-all::Thirdeye_rec3',
      'thirdeye-all::Thirdeye_rec4',
      'thirdeye-all::Thirdeye_rec5'
      ]
    };
  });

}
