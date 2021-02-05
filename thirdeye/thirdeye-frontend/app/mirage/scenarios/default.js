export default function (server) {
  /*
    Seed your development database using your factories.
    This data will not be loaded in your tests.

    Make sure to define a factory for each model you want to create.
  */

  /**
   * Creates a mock anomaly on server start
   */
  server.createList('anomaly', 1);
  server.createList('alert', 1);
}
