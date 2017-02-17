function AnalysisView(analysisModel) {
  // Compile template
  var analysis_template = $("#analysis-template").html();
  this.analysis_template_compiled = Handlebars.compile(analysis_template);
  this.analysisModel = analysisModel;
  this.applyDataChangeEvent = new Event(this);
  this.viewParams = {granularity: "DAYS", dimension: "All", filters: {}};
  this.baselineRange = {
    'Last 24 Hours': [moment().subtract(1, 'days'), moment().subtract(1, 'days')],
    'Yesterday': [moment().subtract(2, 'days'), moment().subtract(2, 'days')],
    'Last 7 Days': [moment().subtract(13, 'days'), moment().subtract(7, 'days')],
    'Last 30 Days': [moment().subtract(59, 'days'), moment().subtract(30, 'days')],
    'This Month': [moment().subtract(1, 'month').startOf('month'), moment().subtract(1 , 'month').endOf('month')],
    'Last Month': [moment().subtract(2, 'month').startOf('month'), moment().subtract(2, 'month').endOf('month')]
  };
}

AnalysisView.prototype = {
  init(params = {}) {
    const { metricId } = params;
    this.viewParams.metricId = metricId || '';
  },

  render: function () {
    $("#analysis-place-holder").html(this.analysis_template_compiled);
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

      this.viewParams['metric'] = {id: metricId, alias: metricAlias, allowClear:true, name:metricName};
      this.viewParams['metricId'] = metricId;

      // Now render the dimensions and filters for selected metric
      this.renderGranularity(this.viewParams.metric.id);
      this.renderDimensions(this.viewParams.metric.id);
      this.renderFilters(this.viewParams.metric.id);
    }).trigger('change');

    // TIME RANGE SELECTION
    var currentStart = this.analysisModel.currentStart;
    var currentEnd = this.analysisModel.currentEnd;
    var baselineStart = this.analysisModel.baselineStart;
    var baselineEnd = this.analysisModel.baselineEnd;

    setCurrentRange(currentStart, currentEnd);
    setBaselineRange(baselineStart, baselineEnd);

    this.renderDatePicker($currentRange, setCurrentRange, currentStart, currentEnd);
    this.renderDatePicker($baselineRange, setBaselineRange, baselineStart, baselineEnd);

    this.setupListeners();
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
        'Last 24 Hours': [moment(), moment()],
        'Yesterday': [moment().subtract(1, 'days'), moment().subtract(1, 'days')],
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
      minimumResultsForSearch: -1,
      data: granularities
    };
    if (granularities) {
      $("#analysis-granularity-input").select2().empty();
      $("#analysis-granularity-input").select2(config).on("change", function (e) {
        var selectedElement = $(e.currentTarget);
        var selectedData = selectedElement.select2("data");
        self.viewParams['granularity'] = selectedData[0].id;
      }).trigger('change');
    }
  },

  renderDimensions: function (metricId) {
    if(!metricId) return;
    var self = this;
    var dimensions = self.analysisModel.fetchDimensionsForMetric(metricId);
    var config = {
      minimumResultsForSearch: -1, data: dimensions
    };
    if (dimensions) {
      $("#analysis-metric-dimension-input").select2().empty();
      $("#analysis-metric-dimension-input").select2(config).on("select2:select", function (e) {
        var selectedElement = $(e.currentTarget);
        var selectedData = selectedElement.select2("data");
        self.viewParams['dimension'] = selectedData[0].id;
      });
    }
  },

  renderFilters: function (metricId) {
    if(!metricId) return;
    var self = this;
    var filters = self.analysisModel.fetchFiltersForMetric(metricId);
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
    var self = this;
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
    self.viewParams['filters'] = filterMap;

    // Also reset the filters for heatmap
    self.viewParams['heatmapFilters'] = filterMap;
  },

  setupListeners: function () {
    var self = this;
    $("#analysis-apply-button").click(function (e) {
      self.collectViewParams();
      self.applyDataChangeEvent.notify();
    });
  }
};
