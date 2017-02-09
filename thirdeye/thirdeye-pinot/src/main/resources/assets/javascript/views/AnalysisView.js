function AnalysisView(analysisModel) {
  // Compile template
  var analysis_template = $("#analysis-template").html();
  this.analysis_template_compiled = Handlebars.compile(analysis_template);
  this.analysisModel = analysisModel;
  this.applyDataChangeEvent = new Event(this);
  this.viewParams = {granularity: "DAYS", dimension: "All", filters: {}};
}

AnalysisView.prototype = {
  init(params = {}) {
    const { metricId } = params;
    this.viewParams.metricId = metricId || '';
  },

  render: function () {
    $("#analysis-place-holder").html(this.analysis_template_compiled);
    var self = this;
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
      const selectedElement = $(e.currentTarget);
      const selectedData = selectedElement.select2('data');
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
      this.renderGranularity(self.viewParams.metric.id);
      this.renderDimensions(self.viewParams.metric.id);
      this.renderFilters(self.viewParams.metric.id);
    }).trigger('change');

    // TIME RANGE SELECTION
    var current_start = self.analysisModel.currentStart
    var current_end = self.analysisModel.currentEnd;
    var baseline_start = self.analysisModel.baselineStart;
    var baseline_end = self.analysisModel.baselineEnd;

    current_range_cb(current_start, current_end);
    baseline_range_cb(baseline_start, baseline_end);

    this.renderDatePicker('#current-range', current_range_cb, current_start, current_end);
    this.renderDatePicker('#baseline-range', baseline_range_cb, baseline_start, baseline_end);

    function current_range_cb(start, end) {
      self.viewParams['currentStart'] = start;
      self.viewParams['currentEnd'] = end;
      $('#current-range span').addClass("time-range").html(
          start.format('MMM D, ') + start.format('hh:mm a') + '  &mdash;  ' + end.format('MMM D, ')
          + end.format('hh:mm a'));
    }

    function baseline_range_cb(start, end) {
      self.viewParams['baselineStart'] = start;
      self.viewParams['baselineEnd'] = end;
      $('#baseline-range span').addClass("time-range").html(
          start.format('MMM D, ') + start.format('hh:mm a') + '  &mdash;  ' + end.format('MMM D, ')
          + end.format('hh:mm a'));
    }

    this.setupListeners();
  },

  renderDatePicker: function (domId, callbackFun, initialStart, initialEnd){
    $(domId).daterangepicker({
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
