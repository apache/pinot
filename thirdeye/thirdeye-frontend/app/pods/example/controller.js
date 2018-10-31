import Controller from '@ember/controller';

export default Controller.extend({
  splitView: false,
  actions: {
    onToggleSplitView() {
      this.toggleProperty('splitView');
    }
  }
});
