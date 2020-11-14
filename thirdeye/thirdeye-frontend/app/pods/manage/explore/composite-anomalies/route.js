import Route from "@ember/routing/route";
import AuthenticatedRouteMixin from "ember-simple-auth/mixins/authenticated-route-mixin";

export default Route.extend(AuthenticatedRouteMixin, {
  /**
   * Ember Route life hook
   *
   * @override
   *
   * @return {Object}
   *   The model for this route
   */
  model() {
    return this.modelFor('manage.explore');
  },

  /**
   * Perform initialization of the controller after the model is available.
   *
   * @override
   */
  setupController(controller, model){
    this._super(controller, model);

    //Cannot rely on the native `init` callback of the controller because the model is not available before controller initialization
    this.controller.activate();
  }
});
