function AnomalyFilterModel() {
  this.allAnomalyIds = [];
  this.selectedAnomalyIds = [];
  this.selectedAnomalies = new Map([]);
  this.selectedFilters = new Set([]);
  this.expandedFilters = new Set([]);
  this.searchFilters = {};
  this.viewFilters = null;
  this.hiddenFilters = ['statusFilterMap'];
}

AnomalyFilterModel.prototype = {
  init(params = {}) {
    const {
      anomalyIds,
      searchFilters
    } = params;
    this.anomalyIds = new Set(anomalyIds);
    this.searchFilters = searchFilters;
  },

  resetSelection() {
    this.selectedAnomalyIds = [];
  },

  clear() {
    this.selectedAnomalyIds = [];
    this.selectedAnomalies = new Map([]);
    this.selectedFilters = new Set([]);
    this.expandedFilters = new Set([]);
    this.viewFilters = null;
  },

  filtersIterator(obj, func, formatter = false) {
    let acc = {};
    Object.keys(obj).forEach((key) => {
      let result = Array.isArray(obj[key]) ?
          func(key, obj[key]) :
          this.filtersIterator(obj[key], func, formatter);

      result = (result && formatter) ? formatter(key, result) : result;
      if (result) {
        acc[key] = result;
      }
    });
    return Object.keys(acc).length ? acc : false;
  },

  getSelectedFilters() {
    return [...this.selectedAnomalies.keys()].map((key) => key.split('/'));
  },

  /**
   *
   * Return non empty filters
   * @return {Object} Subset of searchFilters
   */
  getAnomaliesFilters() {
    const anomaliesFilters = this.viewFilters || this.searchFilters;

    return this.filtersIterator(
      anomaliesFilters,
      (key, anomalyIds) => {
        if (!anomalyIds.length) return;
        // if (this.selectedAnomalies.has(section/key)) {
        //   anomalyIds.selected = true;
        // }
        return anomalyIds;
      },
      (key, result) => {
        if (!result || this.hiddenFilters.includes(key)) {
          return false;
        }
        if (this.expandedFilters.has(key)) {
          result.expanded = true;
        }

        //
        return result;
      }
    );
  },

  getIntersection(set1, set2) {
    set1 = new Set(set1);
    set2 = new Set(set2);

    if (set1.size) {
      return new Set([...set1].filter(anomalyId => set2.has(anomalyId)));
    }
    return new Set(set2);
  },

  updateSelectedAnomalyIds(anomalies) {
    this.selectedAnomalyIds = [...this.selectedAnomalies.values()].reduce((acc, anomalyIds) => {
      return [...this.getIntersection(acc, anomalyIds)];
    }, []);
  },

  updateViewFilters() {
    this.viewFilters = this.filtersIterator(this.searchFilters, (filterName, anomalyIds) => {
      return [...this.getIntersection(this.selectedAnomalyIds, anomalyIds)];
    });
  },

  addFilter(filter, section) {
    // const filters = this.searchFilters;
    const selectedAnomalyIds = this.selectedAnomalyIds;
    this.filtersIterator(
      this.searchFilters,
      (filterName, anomalyIds) => {
        if (filterName === filter) {
          return anomalyIds;
        }
      },
      (key, value) => {
        if (key === filter) {
          return value;
        }
        if (key === section) {
          this.selectedAnomalies.set(`${section}/${filter}`, value[filter]);
        }
      }
    );
    this.updateSelectedAnomalyIds();
    this.updateViewFilters();

    console.log('selected anomalids: ', this.selectedAnomalyIds);
  },

  removeFilter(filter, section) {
    this.selectedAnomalies.delete(`${section}/${filter}`);
    this.updateSelectedAnomalyIds();
    this.updateViewFilters();
  },

  updatefilterSection(filter) {
    this.expandedFilters.has(filter) ?
      this.expandedFilters.delete(filter) :
      this.expandedFilters.add(filter);
  }
};
