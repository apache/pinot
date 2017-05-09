function AnomalyResultView(anomalyResultModel) {

  // model
  this.anomalyResultModel = anomalyResultModel;
  this.metricSearchConfig = {
      theme : "bootstrap",
      width: '100%',
      allowClear: true,
      placeholder : "Search for Metric",
      ajax : {
        url : '/data/autocomplete/metric',
        minimumInputLength : 3,
        delay : 250,
        data : function(params) {
          var query = {
            name : params.term,
            page : params.page
          }
          // Query paramters will be ?name=[term]&page=[page]
          return query;
        },
        processResults : function(data) {
          var results = [];
          const mode = $('#anomalies-search-mode').val();
            $.each(data, function(index, item) {
              results.push({
                id : item.id,
                text : item.alias
              });
            });
          return {
            results : results
          };
        }
      }
    };
  this.dashboardSearchConfig = {
      theme : "bootstrap",
      width: '100%',
      allowClear: true,
      placeholder : "Search for Dashboard",
      ajax : {
        url : '/data/autocomplete/dashboard',
        minimumInputLength : 3,
        delay : 250,
        data : function(params) {
          var query = {
            name : params.term,
            page : params.page
          }
          // Query paramters will be ?name=[term]&page=[page]
          return query;
        },
        processResults : function(data) {
          var results = [];
          const mode = $('#anomalies-search-mode').val();
            $.each(data, function(index, item) {
              results.push({
                id : item.id,
                text : item.name
              });
            });
          return {
            results : results
          };
        }
      }
    };

  this.anomalySearchConfig = {
      theme : "bootstrap",
      width: '100%',
      allowClear: true,
      placeholder : "Search for anomaly ID",
      tags: true
    };
  this.groupSearchConfig = {
      theme : "bootstrap",
      width: '100%',
      allowClear: true,
      placeholder : "Search for group ID",
      tags: true
    };
  this.timeSearchConfig = {
      theme : "bootstrap",
      width: '100%',
      placeholder : "All anomalies in the selected time range",
      tags: true,
      disabled: true
    };

  this.timeRangeConfig = {
    startDate : this.anomalyResultModel.startDate,
    endDate : this.anomalyResultModel.endDate,
    dateLimit : {
      days : 60
    },
    showDropdowns : true,
    showWeekNumbers : true,
    timePicker : true,
    timePickerIncrement : 60,
    timePicker12Hour : true,
    ranges: {
      'Last 24 Hours': [moment().subtract(1, 'days'), moment()],
      'Yesterday': [moment().subtract(1, 'days').startOf('day'), moment().subtract(1, 'days').endOf('day')],
      'Last 7 Days': [moment().subtract(6, 'days').startOf('day'), moment().endOf('day')],
      'Last 30 Days': [moment().subtract(29, 'days').startOf('day'), moment().endOf('day')],
      'This Month': [moment().startOf('month'), moment().endOf('month')],
      'Last Month': [moment().subtract(1, 'month').startOf('month'),
        moment().subtract(1, 'month').endOf('month')]
    },
    buttonClasses : [ 'btn', 'btn-sm' ],
    applyClass : 'btn-primary',
    cancelClass : 'btn-default'
  };

  // Compile HTML template
  const anomalies_template = $("#anomalies-template").html();
  this.anomalies_template_compiled = Handlebars.compile(anomalies_template);
  $("#anomalies-place-holder").html(this.anomalies_template_compiled);

  const anomaly_results_template = $("#anomaly-results-template").html();
  this.anomaly_results_template_compiled = Handlebars.compile(anomaly_results_template);

  // events
  this.applyButtonEvent = new Event(this);
  this.checkedFilterEvent = new Event(this);
  this.filterButtonEvent = new Event(this);
  this.resetButtonEvent = new Event(this);
  this.investigateButtonClickEvent = new Event(this);
  this.showDetailsLinkClickEvent = new Event(this);
  this.anomalyFeedbackSelectEvent = new Event(this);
  this.pageClickEvent = new Event(this);

  this.anomalyResultModel.renderViewEvent.attach(this.renderViewEventHandler.bind(this));
}

AnomalyResultView.prototype = {
  init : function() {
    var self = this;
    $('#anomalies-search-mode').select2({
      minimumResultsForSearch : -1,
      theme : "bootstrap"
    }).on("change", function(e) {
      self.showSearchBarBasedOnMode();
    });

    this.setupSearchBar();
    this.showSearchBarBasedOnMode();
    this.setupSearchListener();
  },
  // *** should send data here to handle conditional rendering?
  renderViewEventHandler : function() {
    this.render();
  },

  render() {
    this.renderAnomaliesTab();
    // this.renderSearchFilters();
    this.showSearchBarBasedOnMode();
    this.renderPagination();

    // FUNCTION DROPDOWN
    var functions = this.anomalyResultModel.getAnomalyFunctions();
    var anomalyFunctionSelector = $('#anomaly-function-dropdown');
    $.each(functions, function(val, text) {
      anomalyFunctionSelector.append($('<option></option>').val(val).html(text));
    });
  },

  // should have 2type of destroy
  // need to destroy date picker
  destroy() {
    $('#anomaly-results-place-holder').children().remove();
  },

  destroyDatePickers() {
    const $currentRangePicker = $('#current-range');
    const $baselineRangePicker = $('#baseline-range');
    $currentRangePicker.length && $currentRangePicker.data('daterangepicker').remove();
    $baselineRangePicker.length && $baselineRangePicker.data('daterangepicker').remove();
  },
  showSearchBarBasedOnMode : function() {
    var anomaliesSearchMode = $('#anomalies-search-mode').val();
    $('#anomalies-search-dashboard-container').hide();
    $('#anomalies-search-anomaly-container').hide();
    $('#anomalies-search-metrics-container').hide();
    $('#anomalies-search-time-container').hide();
    $('#anomalies-search-group-container').hide();
    if (anomaliesSearchMode == constants.MODE_METRIC) {
      $('#anomalies-search-metrics-container').show();
    } else if (anomaliesSearchMode == constants.MODE_DASHBOARD) {
      $('#anomalies-search-dashboard-container').show();
    } else if (anomaliesSearchMode == constants.MODE_ID) {
      $('#anomalies-search-anomaly-container').show();
    } else if (anomaliesSearchMode == constants.MODE_TIME) {
      $('#anomalies-search-time-container').show();
    } else if (anomaliesSearchMode == constants.MODE_GROUPID) {
      $('#anomalies-search-group-container').show();
    }
  },
  setupSearchBar : function() {
    $('#anomalies-search-metrics-input').select2(this.metricSearchConfig).on("select2:select", function(e) {
    });
    $('#anomalies-search-dashboard-input').select2(this.dashboardSearchConfig).on("select2:select", function(e) {
    });
    $('#anomalies-search-anomaly-input').select2(this.anomalySearchConfig).on("select2:select", function(e) {
    });
    $('#anomalies-search-group-input').select2(this.groupSearchConfig).on("select2:select", function(e) {
    });
    $('#anomalies-search-time-input').select2(this.timeSearchConfig).on("select2:select", function(e) {
    });
  },

  renderAnomaliesTab : function() {
    const anomaliesWrapper = this.anomalyResultModel.getAnomaliesWrapper();
    const anomaly_results_template_compiled_with_results = this.anomaly_results_template_compiled(anomaliesWrapper);
    $("#anomaly-results-place-holder").html(anomaly_results_template_compiled_with_results);
    const anomalies = anomaliesWrapper.anomalyDetailsList;
    anomalies.forEach((anomaly, index) => {

      if (!anomaly) {
        return;
      }
      const date = ['date'].concat(anomaly.dates);
      const currentValues = ['current'].concat(anomaly.currentValues);
      const baselineValues = ['baseline'].concat(anomaly.baselineValues);
      const chartColumns = [ date, currentValues, baselineValues ];
      const showPoints = date.length <= constants.MAX_POINT_NUM;


      // CHART GENERATION
      var chart = c3.generate({
        bindto : '#anomaly-chart-' + index,
        data : {
          x : 'date',
          xFormat : '%Y-%m-%d %H:%M',
          columns : chartColumns,
          type : 'line'
        },
        point: {
          show: showPoints,
        },
        legend : {
          position : 'inset',
          inset: {
            anchor: 'top-right',
          }
        },
        axis : {
          y : {
            show : true,
            tick: {
              format: d3.format('.2s')
            }
          },
          x : {
            type : 'timeseries',
            show : true,
            tick: {
              fit: false,
            }
          }
        },
        regions : [ {
          axis : 'x',
          start : anomaly.anomalyRegionStart,
          end : anomaly.anomalyRegionEnd,
          class: 'anomaly-region',
          tick : {
            format : '%m %d %Y'
          }
        } ]
      });

      this.setupListenersOnAnomaly(index, anomaly);
    })
  },

  renderSearchFilters() {
    if (this.searchFilters) return;

    const anomaliesFilters = this.anomalyResultModel.getAnomaliesFilters();
    const anomaly_filters_compiled = this.anomaly_filters_template_compiled({ anomaliesFilters });
    this.destroyDatePickers();
    $('#anomaly-filters-place-holder').children().remove();
    $('#anomaly-filters-place-holder').html(anomaly_filters_compiled);
     // TIME RANGE SELECTION
    this.timeRangeConfig.startDate = this.anomalyResultModel.startDate;
    this.timeRangeConfig.endDate = this.anomalyResultModel.endDate;

    const $anomalyTimeRangeStart = $('#anomalies-time-range-start span');
    const $anomalyTimeRangeEnd = $('#anomalies-time-range-end span');

    function cb(start, end, rangeType = constants.DATE_RANGE_CUSTOM) {
      $anomalyTimeRangeStart.addClass('time-range').html(start.format(constants.DATE_TIME_RANGE_FORMAT));
      $anomalyTimeRangeEnd.addClass('time-range').html(end.format(constants.DATE_TIME_RANGE_FORMAT));
      console.log("changed")
    }
    $('#anomalies-time-range-start').daterangepicker(this.timeRangeConfig, cb);

    $('#anomalies-time-range-end').on('click', () => {
      $('#anomalies-time-range-start').click();
    });

    // $('#anomalies-time-range-end').daterangepicker(this.timeRangeConfig, cb);
    cb(this.timeRangeConfig.startDate, this.timeRangeConfig.endDate);
    this.setupFilterListener();

    // APPLY BUTTON
  },

  renderPagination() {
    const anomaliesWrapper = this.anomalyResultModel.getAnomaliesWrapper();
    const totalAnomalies = anomaliesWrapper.totalAnomalies;
    const pageSize = this.anomalyResultModel.pageSize;
    const pageNumber = this.anomalyResultModel.pageNumber;
    const numPages = Math.ceil(totalAnomalies / pageSize);

    // Don't display pagination when result is empty
    if (!totalAnomalies) {
      return;
    }

    $('#pagination').twbsPagination({
      totalPages: numPages,
      visiblePages: 7,
      startPage: pageNumber,
      onPageClick: (event, page) => {
        $('body').scrollTop(0);
        if (page != pageNumber) {
          this.pageClickEvent.notify(page);
        }
      }
    });
  },

  dataEventHandler : function(e) {
    e.preventDefault();
    var currentTargetId = e.currentTarget.id;
    if (currentTargetId.startsWith('investigate-button-')) {
      this.investigateButtonClickEvent.notify(e.data);
    } else if (currentTargetId.startsWith('show-details-')) {
      this.showDetailsLinkClickEvent.notify(e.data);
    }
  },

  getSearchParams() {
    var anomaliesSearchMode = $('#anomalies-search-mode').val();
    var metricIds = undefined;
    var dashboardId = undefined;
    var anomalyIds = undefined;
    var functionName = $('#anomaly-function-dropdown').val();

    // const $anomalyDatePicker = $('#anomalies-time-range-start');
    // const dateRangeData = $anomalyDatePicker.data('daterangepicker');

    // uses default startDate and endDate
    const startDate = this.anomalyResultModel.startDate;
    const endDate = this.anomalyResultModel.endDate;

    const anomaliesParams = {
      anomaliesSearchMode: anomaliesSearchMode,
      startDate: startDate,
      endDate: endDate,
      pageNumber: 1,
      functionName: functionName
    }

    if (anomaliesSearchMode == constants.MODE_METRIC) {
      anomaliesParams.metricIds = $('#anomalies-search-metrics-input').val().join();
    } else if (anomaliesSearchMode == constants.MODE_DASHBOARD) {
      anomaliesParams.dashboardId = $('#anomalies-search-dashboard-input').val();
    } else if (anomaliesSearchMode == constants.MODE_ID) {
      anomaliesParams.anomalyIds = $('#anomalies-search-anomaly-input').val().join();
      delete anomaliesParams.startDate;
      delete anomaliesParams.endDate;
    } else if (anomaliesSearchMode == constants.MODE_GROUPID) {
        anomaliesParams.anomalyGroupIds = $('#anomalies-search-group-input').val().join();
        delete anomaliesParams.startDate;
        delete anomaliesParams.endDate;
    }

    return anomaliesParams;
  },

  setupSearchListener() {
    $('#search-button').click(() => {
      this.searchFilters = null;
      const anomaliesParams = this.getSearchParams();
      anomaliesParams.searchFilters = null;
      this.applyButtonEvent.notify(anomaliesParams);
    })
  },

  setupFilterListener() {
    // search with filters
    $('#apply-button').click(() => {
      $('body').scrollTop(0);
      // getting all checked
      this.searchFilters = Object.assign({}, this.anomalyResultModel.getAnomaliesFilters());
      // const filterType = Object.keys(filters);
      const searchFilters = {};

      // for each prop in the filters, find the checked and update
      // $("#functionFilterMap").find(".filter-item__checkbox:checked");
      const checkedFilters = $('.filter-item__checkbox:checked');
// get the parent
//
//
// ------***** coding from
//
const newSearchFilters = {};
//getting parents with all level
const section = $('.filter-section:not(".filter-section--no-padding")').has('.filter-item__checkbox:checked');

section.each((index, section) => {
  newSearchFilters[section.id] = {};
  const selectedFilters = $(section).find('.filter-item__checkbox:checked');
  selectedFilters.each((index, filter) => {
    const filterBlockName = filter.closest('.filter-section').id;
    if (filterBlockName !== section.id) {
      newSearchFilters[section.id][filterBlockName] = newSearchFilters[section.id][filterBlockName] || {};
      newSearchFilters[section.id][filterBlockName][filter.id] = 1;
    } else {
      newSearchFilters[section.id][filter.id] = 1;
    }
  });
});

// do something with newSearchFilters
  // anomaliesParams.searchFilters = newSearchFilters;
  // constructing filter

  this.applyButtonEvent.notify({ searchFilters: newSearchFilters })
  });
// ------*****
// $('.filter-section').has('.filter-section>.filter-item__checkbox:checked')
    //   checkedFilters.each((index, filter) => {
    //     const filterType = filter.closest('.filter-section').id
    //     if (searchFilters[filterType] || allFilters.hasOwnProperty(filterType)) {
    //       searchFilters[filterType] = searchFilters[filterType] || {};
    //       searchFilters[filterType][filter.id] = 1;
    //     }
    //   })
    //   // getting time
    //   const anomaliesParams = this.getSearchParams();
    //   anomaliesParams.searchFilters = searchFilters;
    //   // constructing filter

    //   this.applyButtonEvent.notify(anomaliesParams)
    //   // update view
    // })

    // search with no filters
    $('.filter-section').on('click', '.filter-title', function(event) {
      $(event.delegateTarget).children('.filter-body__list').toggleClass('filter-body__list--hidden')
      $(this).find('.filter-title__action').toggleClass('filter-title__action--expanded');
      event.stopPropagation()
    })

    // on check for filters
    $('.filter-section').on('click', '.filter-item__checkbox', (event) => {
      const {
        id: filter,
        checked: isChecked
      } = event.target;

      debugger;
      this.checkedFilterEvent.notify({ filter, isChecked});
      // filter through all anomalies
      //
      // keep a selected anomalies
      //
      // keep a filter data structure
      //
      // keep all filters
    })
  },

  setupListenersOnAnomaly : function(idx, anomaly) {
    const investigateParams = {
      anomalyId : anomaly.anomalyId,
    }

    $(`#investigate-button-${idx}`).click(investigateParams, this.dataEventHandler.bind(this));

    var rootCauseAnalysisParams = {
      metric : anomaly.metric,
      currentStart : anomaly.currentStart,
      currentEnd : anomaly.currentEnd,
      baselineStart: anomaly.baselineStart,
      baselineEnd: anomaly.baselineEnd,
      // not needed at the moment since it returns {}
      // dimension : anomaly.anomalyFunctionDimension
    }
    $('#root-cause-analysis-button-' + idx).click(rootCauseAnalysisParams, this.dataEventHandler.bind(this));
    var showDetailsParams = {
      anomalyId : anomaly.anomalyId,
      metric : anomaly.metric,
      rangeStart : anomaly.currentStart,
      rangeEnd : anomaly.currentEnd,
      dimension : anomaly.anomalyFunctionDimension
    }
    $('#show-details-' + idx).click(showDetailsParams, this.dataEventHandler.bind(this));
    var anomalyFeedbackParams = {
      idx : idx,
      anomalyId : anomaly.anomalyId
    }
    $('#anomaly-feedback-' + idx).change(anomalyFeedbackParams, this.dataEventHandler.bind(this));
  }


};

