import Route from '@ember/routing/route';

/**
 * @summary A Index Route to serve `home` as the default home page upon login (with no target)
 */
export default Route.extend({
  beforeModel(/*transition*/) {
    //default to home if no intented target route
    this.replaceWith('home');
  }
});
