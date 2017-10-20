import Ember from 'ember';

export default Ember.Component.extend({
  init() {
    this._super(...arguments);
    const filterBlocks = this.get('filterBlocks');

    // Set up filter block object
    filterBlocks.forEach((block) => {
      block.filtersArray = [];
      block.isHidden = false;
      // Dedupe and remove null or empty values
      block.filterKeys = Array.from(new Set(block.filterKeys.filter(value => Ember.isPresent(value))));
      block.filterKeys.forEach((filter) => {
        block.filtersArray.push({
          isActive: false,
          name: filter,
          id: filter.dasherize()
        });
      });
    });
  },

  alertFilters: [],

  actions: {
    /**
     * Handles selection of filter items. Each time a filter is selected, this component will
     * pass an array of selected filters to its parent.
     */
    onFilterSelection(category, filterObj) {
      Ember.set(filterObj, 'isActive', !filterObj.isActive);

      if (filterObj.isActive) {
        this.get('alertFilters').push({ category, filter: filterObj.name, isActive: filterObj.isActive });
      } else {
        this.set('alertFilters', _.reject(this.get('alertFilters'), function(filter) { return filter.filter === filterObj.name; }));
      }

      // Return the 'userDidSelectFilter' function passed from the parent as the value of onSelectFilter, and invoke it.
      this.get('onSelectFilter')(this.get('alertFilters'));
    },

    toggleDisplay(block) {
      Ember.set(block, 'isHidden', !block.isHidden);
    }
  }
});
