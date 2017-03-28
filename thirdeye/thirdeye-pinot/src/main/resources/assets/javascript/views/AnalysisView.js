function AnalysisView(analysisModel) {
  // Compile template
  const analysis_template = $("#analysis-template").html();
  this.analysis_template_compiled = Handlebars.compile(analysis_template);

  this.analysisModel = analysisModel;
  this.applyDataChangeEvent = new Event(this);
  this.searchEvent = new Event(this);
  this.viewParams = {granularity: "DAYS", dimension: "All", filters: {}};
  this.baselineRange = {
    'Last 24 Hours': [moment().subtract(2, 'days'), moment().subtract(1, 'days')],
    'Yesterday': [moment().subtract(3, 'days'), moment().subtract(2, 'days')],
    'Last 7 Days': [moment().subtract(13, 'days'), moment().subtract(7, 'days')],
    'Last 30 Days': [moment().subtract(59, 'days'), moment().subtract(30, 'days')],
    'This Month': [moment().subtract(1, 'month').startOf('month'), moment().subtract(1 , 'month').endOf('month')],
    'Last Month': [moment().subtract(2, 'month').startOf('month'), moment().subtract(2, 'month').endOf('month')]
  };
}

AnalysisView.prototype = {
  init(params = {}) {
    this.viewParams = params;
    this.searchParams = {};
    this.viewParams.metricName = this.analysisModel.metricName;
  },

  render: function (metricId) {
    $("#analysis-place-holder").html(this.analysis_template_compiled);
    // METRIC SELECTION
    var analysisMetricSelect = $('#analysis-metric-input').select2({
      theme: "bootstrap", placeholder: "search for Metric(s)", ajax: {
        url: constants.METRIC_AUTOCOMPLETE_ENDPOINT, delay: 250, data: function (params) {
          var query = {
            name: params.term, page: params.page
          };
          // Query parameters will be ?search=[term]&page=[page]
          return query;
        }, processResults: function (data) {
          var results = [];
          $.each(data, function (index, item) {
            results.push({
              id: item.id,
              text: item.alias,
              name: item.name
            });
          });
          return {
            results: results
          };
        }
      }
    });

    analysisMetricSelect.on('change', (e) => {
      const $selectedElement = $(e.currentTarget);
      const selectedData = $selectedElement.select2('data');
      let metricId;
      let metricAlias;
      let metricName;

      if (selectedData.length) {
        const {id, text, name} = selectedData[0];
        metricId = id;
        metricAlias = text;
        metricName = name;
      }

      this.searchParams['metric'] = {id: metricId, alias: metricAlias, allowClear:true, name:metricName} || this.searchParams['metric'];
      this.searchParams['metricId'] = metricId || this.searchParams['metricId'];
      this.searchParams['metricName'] = metricName || this.searchParams['metricName'];

    }).trigger('change');
    if (metricId) {
      this.renderAnalysisOptions(metricId);
    }
    this.setupSearchListeners();
  },

  renderAnalysisOptions(metricId) {
    metricId = metricId || Object.assign(this.viewParams, this.searchParams).metricId;
    // Now render the dimensions and filters for selected metric

    const analysis_options_template = $('#analysis-options-template').html();
    const compiled_analysis_options_template = Handlebars.compile(analysis_options_template);
    $('#analysis-options-placeholder').html(compiled_analysis_options_template);

    this.renderDateRangePickers();
    this.renderGranularity(metricId);
    this.renderDimensions(metricId);
    this.renderFilters(metricId);
    this.setupApplyListener(metricId);
  },

  renderDateRangePickers() {
    const $currentRangeText = $('#current-range span');
    const $baselineRangeText= $('#baseline-range span');
    const $currentRange = $('#current-range');
    const $baselineRange = $('#baseline-range');

    const setBaselineRange = (start, end) => {
      this.viewParams['baselineStart'] = start;
      this.viewParams['baselineEnd'] = end;
      $baselineRangeText.addClass("time-range").html(
        `${start.format(constants.DATE_RANGE_FORMAT)} &mdash; ${end.format(constants.DATE_RANGE_FORMAT)}`);
    };
    const setCurrentRange = (start, end, rangeType = constants.DATE_RANGE_CUSTOM) => {
      this.viewParams['currentStart'] = start;
      this.viewParams['currentEnd'] = end;

      if (rangeType === constants.DATE_RANGE_CUSTOM) {
        $baselineRange.removeClass('disabled');
      } else {
        const [baselineStart, baselineEnd] = this.baselineRange[rangeType];
        $baselineRange.addClass('disabled');
        setBaselineRange(baselineStart, baselineEnd);
      }
      $currentRangeText.addClass("time-range").html(
          `<span>${rangeType}</span> ${start.format(constants.DATE_RANGE_FORMAT)} &mdash; ${end.format(constants.DATE_RANGE_FORMAT)}`)
    };

    // TIME RANGE SELECTION
    var currentStart = this.analysisModel.currentStart;
    var currentEnd = this.analysisModel.currentEnd;
    var baselineStart = this.analysisModel.baselineStart;
    var baselineEnd = this.analysisModel.baselineEnd;

    setCurrentRange(currentStart, currentEnd);
    setBaselineRange(baselineStart, baselineEnd);

    this.renderDatePicker($currentRange, setCurrentRange, currentStart, currentEnd);
    this.renderDatePicker($baselineRange, setBaselineRange, baselineStart, baselineEnd);
  },

  renderDatePicker: function ($selector, callbackFun, initialStart, initialEnd){
    $selector.daterangepicker({
      startDate: initialStart,
      endDate: initialEnd,
      dateLimit: {
        days: 60
      },
      showDropdowns: true,
      showWeekNumbers: true,
      timePicker: true,
      timePickerIncrement: 5,
      timePicker12Hour: true,
      ranges: {
        'Last 24 Hours': [moment().subtract(1, 'days'), moment()],
        'Yesterday': [moment().subtract(2, 'days'), moment().subtract(1, 'days')],
        'Last 7 Days': [moment().subtract(6, 'days'), moment()],
        'Last 30 Days': [moment().subtract(29, 'days'), moment()],
        'This Month': [moment().startOf('month'), moment().endOf('month')],
        'Last Month': [moment().subtract(1, 'month').startOf('month'),
          moment().subtract(1, 'month').endOf('month')]
      },
      buttonClasses: ['btn', 'btn-sm'],
      applyClass: 'btn-primary',
      cancelClass: 'btn-default'
    }, callbackFun);
  },

  renderGranularity: function (metricId) {
    if(!metricId) return;
    var self = this;
    var granularities = self.analysisModel.fetchGranularityForMetric(metricId);
    var config = {
      theme: "bootstrap",
      minimumResultsForSearch: -1,
      data: granularities
    };
    if (granularities) {
      $("#analysis-granularity-input").select2().empty();
      $("#analysis-granularity-input").select2(config);
    }
  },

  renderDimensions: function (metricId) {
    if(!metricId) return;
    var dimensions = this.analysisModel.fetchDimensionsForMetric(metricId);
    var config = {
      theme: "bootstrap",
      minimumResultsForSearch: -1,
      data: dimensions
    };
    if (dimensions) {
      $("#analysis-metric-dimension-input").select2().empty();
      $("#analysis-metric-dimension-input").select2(config);
    }
  },

  renderFilters: function (metricId) {
    if(!metricId) return;
    var filters = this.analysisModel.fetchFiltersForMetric(metricId);
    var filterData = [];
    for (var key in filters) {
      // TODO: introduce category
      var values = filters[key];
      var children = [];
      for (var i in values) {
        children.push({id:key +":"+ values[i], text:values[i]});
      }
      filterData.push({text:key, children:children});
    }
    if (filters) {
      var config = {
        theme: "bootstrap",
        placeholder: "select filter",
        allowClear: false,
        multiple: true,
        data: filterData
      };
      $("#analysis-metric-filter-input").select2().empty();
      $("#analysis-metric-filter-input").select2(config);
    }
  },

  collectViewParams : function () {
    // Collect filters
    var selectedFilters = $("#analysis-metric-filter-input").val();
    var filterMap = {};
    for (var i in selectedFilters) {
      var filterStr = selectedFilters[i];
      var keyVal = filterStr.split(":");
      var list = filterMap[keyVal[0]];
      if (list) {
        filterMap[keyVal[0]].push(keyVal[1]);
      } else {
        filterMap[keyVal[0]] = [keyVal[1]];
      }
    }

    try {
      this.viewParams['dimension'] = $("#analysis-metric-dimension-input").select2("data")[0].id;
      this.viewParams['granularity'] = $("#analysis-granularity-input").select2("data")[0].id;
    } catch (e) {}
    this.viewParams['filters'] = filterMap;

    // Also reset the filters for heatmap
    this.viewParams['heatmapFilters'] = filterMap;
  },

  setupSearchListeners() {
    $("#analysis-search-button").click(() => {
      this.searchEvent.notify();
    });
  },

  setupApplyListener(){
    $("#analysis-apply-button").click((e) => {
      this.collectViewParams();
      this.applyDataChangeEvent.notify();
    });
  }
};
