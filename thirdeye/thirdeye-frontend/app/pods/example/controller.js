import Ember from 'ember';

export default Ember.Controller.extend({
  splitView: false,
  actions: {
    onToggleSplitView() {
      this.toggleProperty('splitView');
    }
  }
});
