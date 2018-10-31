function AnomalyFilterView(anomalyFilterModel) {
  const anomaly_filters_template = $("#anomaly-filters-template").html();

  this.anomalyFilterModel = anomalyFilterModel;
  this.anomaly_filters_template_compiled = Handlebars.compile(anomaly_filters_template);
  this.checkedFilterEvent = new Event(this);
  this.expandedFilterEvent = new Event(this);
  this.clearEvent = new Event(this);
}

AnomalyFilterView.prototype = {

  /**
   * renders the filters and set up event listeners
   */
  render() {
    const anomaliesFilters = this.anomalyFilterModel.getAnomaliesFilters();
    const anomaly_filters_compiled = this.anomaly_filters_template_compiled({ anomaliesFilters });
    $('#anomaly-filters-place-holder').children().remove();
    $('#anomaly-filters-place-holder').html(anomaly_filters_compiled);

    this.setupFilterListener();
    this.checkSelectedFilters();

  },

  /**
   * Parses through selected Filters and checks then in the UI
   */
  checkSelectedFilters() {
    const selectedFilters = this.anomalyFilterModel.getSelectedFilters();
    selectedFilters.forEach((filter) => {
      const [section, filterName] = filter;
      $(`#${section} .filter-item__checkbox[data-filter="${filterName}"]`).prop('checked', true);
    });
  },

  /**
   * Setup listeners on filters:
   * Expand/Close section on click
   * Add filter on selection
   * Clears Filter
   */
  setupFilterListener() {
    // show hide filter sections
    $('.filter-section').on('click', '.filter-title', (event) => {
      const $section = $(event.delegateTarget);
      const filter = $section.data('section');
      $section.children('.filter-body__list').toggleClass('filter-body__list--hidden');
      $(event.target.parentElement).find('.filter-title__action').toggleClass('filter-title__action--expanded');

      event.stopPropagation();
      this.expandedFilterEvent.notify({ filter });
    });

    // on filter selection
    $('.filter-section').on('click', '.filter-item__checkbox', (event) => {
      const $currentSelection = $(event.target);
      const isChecked = $currentSelection.prop('checked')
      const filter = $currentSelection.data('filter');
      const section = $(event.delegateTarget).data('section');

      this.checkedFilterEvent.notify({
        filter,
        isChecked,
        section
      });
    });

    // clear
    $('#clear-button').click(() => {
      this.clearEvent.notify();
    });
  }
};
