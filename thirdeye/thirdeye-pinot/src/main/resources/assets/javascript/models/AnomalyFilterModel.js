function AnomalyFilterModel() {
  this.allAnomalyIds = [];
  this.selectedAnomalyIds = [];
  this.selectedAnomalies = new Map([]);
  this.expandedFilters = new Set([]);
  this.searchFilters = {};
  this.viewFilters = null;
  this.selectedFilters = null;
  this.pageNumber = 1;
  this.hiddenFilters = [];
}

AnomalyFilterModel.prototype = {
  /**
   * Initialize filter model
   * @param  {Object} properties to be initialized
   */
  init(params = {}) {
    const {
      anomalyIds,
      searchFilters,
      selectedFilters,
      pageNumber
    } = params;
    this.allAnomalyIds = new Set(anomalyIds);
    this.searchFilters = searchFilters;
    this.selectedFilters = selectedFilters;
    this.pageNumber = pageNumber || this.pageNumber;
    if (selectedFilters) {
      this.convertFromHash(selectedFilters);
    }
  },

  /**
   * Resets selected Anomalies
   */
  resetSelection() {
    this.selectedAnomalyIds = [];
  },

  /**
   * Clears seleted filters anomaly information
   */
  clear() {
    this.selectedAnomalyIds = [];
    this.selectedAnomalies = new Map([]);
    this.selectedFilters = new Set([]);
    this.expandedFilters = new Set([]);
    this.viewFilters = null;
  },

  /**
   * Resets all anomalies related information
   */
  reset() {
    this.clear();
    this.pageNumber = 1;
    this.allAnomalyIds = [];
  },

  /**
   * Helper function to traverse the anomaly filter object
   * @param  {Object}   obj       The oject to traverse
   * @param  {Function} func      Callback  to be called  on value
   * @param  {Function} formatter Callback  to be called  on key
   * @return {Object} resulting object after both callback are applied
   */
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

  /**
   * Gets Selected Filters section and Name
   * @return {Array} Array containing [section, filterName]
   */
  getSelectedFilters() {
    return [...this.selectedAnomalies.keys()].map((key) => key.split('::'));
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
        return anomalyIds;
      },
      (key, result) => {
        if (!result || this.hiddenFilters.includes(key)) {
          return false;
        }
        if (this.expandedFilters.has(key)) {
          result.expanded = true;
        }

        return result;
      }
    );
  },

  /**
   * Get Hash params compatible selected filters
   * @return {Obj} Hash params compatible object
   */
  getViewFiltersHash() {
    const selectedFilters = this.getSelectedFilters();

    return this.convertToHash(selectedFilters);
  },

  /**
   * Converts object to hash compatible object
   * @param  {Object} selectedFilters Object to 'hashify'
   * @return {Object}                 hashified filter
   */
  convertToHash(selectedFilters) {
    const hash = {};
    const searchFilters = this.searchFilters;
    selectedFilters.forEach((filterGroup) => {
      const [section, filter] = filterGroup;
        if (searchFilters[section] && searchFilters[section][filter]) {
          hash[section] = hash[section] || [];
          hash[section].push(filter);
        } else {
          Object.keys(searchFilters).forEach((searchFilter) => {
            if (searchFilters[searchFilter][section] && searchFilters[searchFilter][section][filter]) {
              hash[searchFilter] = hash[searchFilter]|| {};
              hash[searchFilter][section] = hash[searchFilter][section] || [];
              hash[searchFilter][section].push(filter);
            }
          });
        }
    });
    return hash;
  },

  /**
   * Converts object from hash
   * @param  {Object} selectedFilters Object to parse
   */
  convertFromHash(selectedFilters) {
    this.filtersIterator(
      selectedFilters,
      (section, filters) => {
        filters.forEach((filter) => {
          this.addFilter(filter, section);
        });
      }
    );
  },

  /**
   * Get Selected Anomalies Ids
   * @return {Array} Anomaly Ids
   */
  getSelectedAnomalyIds() {
    return this.selectedAnomalyIds.length ?
      this.selectedAnomalyIds :
      [...this.allAnomalyIds];
  },

  /**
   * Get Intersection of 2 sets of anomalies
   * @param  {Set} set1 First Set
   * @param  {Set} set2 Second Set
   * @return {Set}      intersection of the 2 sets
   */
  getIntersection(set1, set2) {
    set1 = new Set(set1);
    set2 = new Set(set2);

    if (set1.size) {
      return new Set([...set1].filter(anomalyId => set2.has(anomalyId)));
    }
    return new Set(set2);
  },

  /**
   * Gets intersecting anomaly Ids of all selected filters
   */
  updateSelectedAnomalyIds(anomalies) {
    this.selectedAnomalyIds = [...this.selectedAnomalies.values()]
        .reduce((acc, anomalyIds) => {
          return [...this.getIntersection(acc, anomalyIds)];
        }, []);
  },

  /**
   * Update Views filters by keeping only selected anomalyIds
   */
  updateViewFilters() {
    this.viewFilters = this.filtersIterator(this.searchFilters, (filterName, anomalyIds) => {
      return [...this.getIntersection(this.selectedAnomalyIds, anomalyIds)];
    });
  },

  /**
   * Add filters and update anomalyIds and view Filters
   * @param {String} filter  Filter Name
   * @param {String} section Filter section
   */
  addFilter(filter, section) {
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
          this.selectedAnomalies.set(`${section}::${filter}`, value[filter]);
        }
      }
    );
    this.updateSelectedAnomalyIds();
    this.updateViewFilters();
  },

  /**
   * Removes filters and update AnomalyIDs and view filters
   * @param {String} filter  Filter Name
   * @param {String} section Filter section
   */
  removeFilter(filter, section) {
    this.selectedAnomalies.delete(`${section}::${filter}`);
    this.updateSelectedAnomalyIds();
    this.updateViewFilters();
  },

  /**
   * Expand/close section
   * @param  {String} section filter section name
   */
  updatefilterSection(section) {
    this.expandedFilters.has(section) ?
      this.expandedFilters.delete(section) :
      this.expandedFilters.add(section);
  }
};
