import Ember from 'ember';

export default Ember.Controller.extend({
  splitView: false,
  queryParams: ['compareMode'],

  compareMode: null,
  selectedTab: 'change',
  compareModeOptions: ['WoW', 'Wo2W', 'Wo3W', 'Wo4W'],

  contributionTableMode: 'change',

  actions: {
    /**
     * Toggles the split View for multimetric graphs
     */
    onSplitViewToggling() {
      this.toggleProperty('splitView');
    },
    /**
     * Changes the compare mode
     * @param {String} compareMode baseline compare mode
     */
    onModeChange(compareMode){
      this.set('compareMode', compareMode);
    },
    /**
     * Handles Contribution Table Tab selection
     * @param {String} tab Name of selected Tab
     */
    onTabChange(tab) {
      const currentTab = this.get('selectedTab');
      if (currentTab !== tab) {
        this.set('selectedTab', tab);
      }
    }
  }
});
