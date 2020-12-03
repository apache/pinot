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
    return this.modelFor("manage.explore");
  }
});
