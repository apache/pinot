function AnomalyFilterView(anomalyFilterModel) {
  const anomaly_filters_template = $("#anomaly-filters-template").html();

  this.anomalyFilterModel = anomalyFilterModel;
  this.anomaly_filters_template_compiled = Handlebars.compile(anomaly_filters_template);
  this.checkedFilterEvent = new Event(this);
  this.expandedFilterEvent = new Event(this);
  this.clearEvent = new Event(this);
}

AnomalyFilterView.prototype = {
  destroyDatePickers() {
    const $currentRangePicker = $('#current-range');
    const $baselineRangePicker = $('#baseline-range');
    $currentRangePicker.length && $currentRangePicker.data('daterangepicker').remove();
    $baselineRangePicker.length && $baselineRangePicker.data('daterangepicker').remove();
  },

  render() {
    // if (this.anomalyFilterModel.searchFilters) return;

    const anomaliesFilters = this.anomalyFilterModel.getAnomaliesFilters();
    const anomaly_filters_compiled = this.anomaly_filters_template_compiled({ anomaliesFilters });
    this.destroyDatePickers();
    $('#anomaly-filters-place-holder').children().remove();
    $('#anomaly-filters-place-holder').html(anomaly_filters_compiled);
     // TIME RANGE SELECTION
    //  this.timeRangeConfig.startDate = this.anomalyResultModel.startDate;
    //  this.timeRangeConfig.endDate = this.anomalyResultModel.endDate;

    //  const $anomalyTimeRangeStart = $('#anomalies-time-range-start span');
    //  const $anomalyTimeRangeEnd = $('#anomalies-time-range-end span');

    //  function cb(start, end, rangeType = constants.DATE_RANGE_CUSTOM) {
    //   $anomalyTimeRangeStart.addClass('time-range').html(start.format(constants.DATE_TIME_RANGE_FORMAT));
    //   $anomalyTimeRangeEnd.addClass('time-range').html(end.format(constants.DATE_TIME_RANGE_FORMAT));
    //   console.log("changed")
    // }
    // $('#anomalies-time-range-start').daterangepicker(this.timeRangeConfig, cb);

    // $('#anomalies-time-range-end').on('click', () => {
    //   $('#anomalies-time-range-start').click();
    // });

    // cb(this.timeRangeConfig.startDate, this.timeRangeConfig.endDate);
    this.setupFilterListener();

    // APPLY BUTTON
  },
  // parse through checkSelected filters and check them in the front end
  // To do implement this with hash params
  checkSelectedFilters() {
    const selectedFilters = this.anomalyFilterModel.getSelectedFilters();
    selectedFilters.forEach((filter) => {
      const [section, filterName] = filter;
      $(`#${section} .filter-item__checkbox[data-filter="${filterName}"]`).prop('checked', true);
    });
  },

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
