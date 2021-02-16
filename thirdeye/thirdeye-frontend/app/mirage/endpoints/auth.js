export default function (server) {
  /**
   * Get request for auth
   * @return {Object}
   */
  server.get(`/auth`, () => {
    return {
      name: 'thirdeye_user@example.com',
      groups: []
    };
  });

  /**
   * POST request for auth
   * @return {Object}
   */
  server.post(`/auth/authenticate`, () => {
    return {};
  });
}
