function AnalysisView(analysisModel) {
  // Compile template
  const analysis_template = $("#analysis-template").html();
  this.analysis_template_compiled = Handlebars.compile(analysis_template);

  this.analysisModel = analysisModel;
  this.applyDataChangeEvent = new Event(this);
  this.searchEvent = new Event(this);
  this.viewParams = {
    granularity: "DAYS",
    dimension: "All",
    filters: {},
  };
  this.compareMode = 'WoW';

  this.currentRange = () => {
    return {
      'Last 24 Hours': [moment().subtract(1, 'days'), moment()],
      'Yesterday': [moment().subtract(2, 'days'), moment().subtract(1, 'days')],
      'Last 7 Days': [moment().subtract(6, 'days'), moment()],
      'Last 30 Days': [moment().subtract(29, 'days'), moment()],
      'This Month': [moment().startOf('month'), moment().endOf('month')],
      'Last Month': [moment().subtract(1, 'month').startOf('month'),
        moment().subtract(1, 'month').endOf('month')]
    };
  };

  this.baselineRange = () => {
    const range = {};
    constants.COMPARE_MODE_OPTIONS.forEach((options) => {
      const offset = constants.WOW_MAPPING[options];
      range[options] = [this.calculateBaselineDate('currentStart', offset),this.calculateBaselineDate('currentEnd', offset)];
    });
   return range;
  };
}

AnalysisView.prototype = {
  init(params = {}) {
    this.viewParams = params;
    this.searchParams = {};
    this.viewParams.metricName = this.analysisModel.metricName;
  },

  calculateBaselineDate(dateType, offset) {
    const baseDate = this.viewParams[dateType] || moment();
    return baseDate.clone().subtract(offset, 'days');
  },

  render: function (metricId, callback) {
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
      if (metricId) {
        this.searchEvent.notify();
      }
    }).trigger('change');

    if (metricId) {
      this.analysisModel.fetchAnalysisOptionsData(metricId, 'analysis-spin-area').then(() => {
        return this.renderAnalysisOptions(metricId);
      }).then(callback);
    }
    this.setupSearchListeners();
  },

  destroyAnalysisGraph() {
    $("#timeseries-contributor-placeholder").children().remove();
  },

  destroyAnalysisOptions() {
    this.destroyDatePickers();
    const $analysisPlaceholder = $('#analysis-options-placeholder');
    $analysisPlaceholder.children().remove();
  },

  destroyDatePickers() {
    const $currentRangePicker = $('#current-range');
    const $baselineRangePicker = $('#baseline-range');
    $currentRangePicker.length && $currentRangePicker.data('daterangepicker').remove();
    $baselineRangePicker.length && $baselineRangePicker.data('daterangepicker').remove();
  },

  destroyDimensionTreeMap() {
    $("#timeseries-contributor-placeholder").children().remove();
  },

  renderAnalysisOptions(metricId) {
    metricId = metricId || Object.assign(this.viewParams, this.searchParams).metricId;
    // Now render the dimensions and filters for selected metric
    const showTime = this.viewParams.granularity !== 'DAYS';
    const analysis_options_template = $('#analysis-options-template').html();
    const compiled_analysis_options_template = Handlebars.compile(analysis_options_template);
    $('#analysis-options-placeholder').html(compiled_analysis_options_template);

    this.renderGranularity(metricId);
    this.renderDateRangePickers(showTime);
    this.renderDimensions(metricId);
    this.renderFilters(metricId);
    this.setupApplyListener(metricId);
  },

  renderDateRangePickers(showTime) {
    const $currentRangeText = $('#current-range span');
    const $baselineRangeText = $('#baseline-range span');
    const $currentRange = $('#current-range');
    const $baselineRange = $('#baseline-range');

    const setBaselineRange = (start, end, compareMode = constants.DATE_RANGE_CUSTOM) => {
      this.viewParams['baselineStart'] = start;
      this.viewParams['baselineEnd'] = end;
      this.viewParams['compareMode'] = compareMode;

      $baselineRangeText.addClass("time-range").html(
        `<span>${compareMode}</span> ${start.format(constants.DATE_RANGE_FORMAT)} &mdash; ${end.format(constants.DATE_RANGE_FORMAT)}`);
    };
    const setCurrentRange = (start, end, rangeType = constants.DATE_RANGE_CUSTOM) => {
      const $baselineRangePicker = $('#baseline-range');

      this.viewParams['currentStart'] = start;
      this.viewParams['currentEnd'] = end;

      const compareMode = this.viewParams['compareMode'];

      if (compareMode !== constants.DATE_RANGE_CUSTOM) {
        $baselineRangePicker.length && $baselineRangePicker.data('daterangepicker').remove();
        const offset = constants.WOW_MAPPING[compareMode];
        const baselineStart = start.clone().subtract(offset, 'days');
        const baselineEnd = end.clone().subtract(offset, 'days');
        this.renderDatePicker($baselineRange, setBaselineRange, baselineStart, baselineEnd, showTime, this.baselineRange);
        setBaselineRange(baselineStart, baselineEnd, compareMode);
      }

      $currentRangeText.addClass("time-range").html(
          `<span>${rangeType}</span> ${start.format(constants.DATE_RANGE_FORMAT)} &mdash; ${end.format(constants.DATE_RANGE_FORMAT)}`)
    };

    // TIME RANGE SELECTION
    var currentStart = this.analysisModel.currentStart;
    var currentEnd = this.analysisModel.currentEnd;
    var baselineStart = this.analysisModel.baselineStart;
    var baselineEnd = this.analysisModel.baselineEnd;


    this.renderDatePicker($currentRange, setCurrentRange, currentStart, currentEnd, showTime, this.currentRange);
    this.renderDatePicker($baselineRange, setBaselineRange, baselineStart, baselineEnd, showTime, this.baselineRange);
    setCurrentRange(currentStart, currentEnd);
    setBaselineRange(baselineStart, baselineEnd, this.viewParams.compareMode);
  },

  renderDatePicker: function ($selector, callbackFun, initialStart, initialEnd, showTime, rangeGenerator){
    const ranges = rangeGenerator();
    $selector.daterangepicker({
      startDate: initialStart,
      endDate: initialEnd,
      dateLimit: {
        days: 60
      },
      showDropdowns: true,
      showWeekNumbers: true,
      timePicker: showTime,
      timePickerIncrement: 5,
      timePicker12Hour: true,
      ranges,
      buttonClasses: ['btn', 'btn-sm'],
      applyClass: 'btn-primary',
      cancelClass: 'btn-default'
    }, callbackFun);
  },

  renderGranularity: function (metricId) {
    if(!metricId) return;
    const $granularitySelector = $("#analysis-granularity-input");
    const paramGranularity = this.viewParams.granularity;
    const granularities = this.analysisModel.granularityOptions;
    const config = {
      theme: "bootstrap",
      minimumResultsForSearch: -1,
      data: granularities,
    };
    if (granularities) {
      $granularitySelector.select2().empty();
      $granularitySelector.select2(config);
      // $granularitySelector.on('change', (event) => {
      //   const showTime = event.currentTarget.value !== 'DAYS';
      //   onChangeCallback.call(this, showTime);
      // });
    }
    if (paramGranularity && granularities.includes(paramGranularity)) {
      $granularitySelector.val(paramGranularity).trigger('change');
    }

  },

  renderDimensions: function (metricId) {
    if(!metricId) return;
    const $dimensionSelector = $("#analysis-metric-dimension-input");
    const paramDimension = this.viewParams.dimension;
    const dimensions = this.analysisModel.dimensionOptions;
    const config = {
      theme: "bootstrap",
      minimumResultsForSearch: -1,
      data: dimensions
    };
    if (dimensions) {
      $dimensionSelector.select2().empty();
      $dimensionSelector.select2(config);
    }
    if (paramDimension && dimensions.includes(paramDimension)) {
      $dimensionSelector.val(paramDimension).trigger('change');
    }
  },

  renderFilters: function (metricId) {
    if(!metricId) return;
    const $filterSelector = $("#analysis-metric-filter-input");
    const filtersParam = this.viewParams.filters;
    var filters = this.analysisModel.filtersOptions;
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
      $filterSelector.select2().empty();
      $filterSelector.select2(config);

      if (filtersParam) {
        let paramFilters = [];
        Object.keys(this.viewParams.filters).forEach((key) => {
          this.viewParams.filters[key].forEach((filterName) => {
            if (filters[key] && filters[key].includes(filterName)) {
              paramFilters.push(`${key}:${filterName}`);
            }
          });
        });

        $filterSelector.val(paramFilters).trigger("change");
      }
    }
  },

  collectViewParams : function () {
    // Collect filters
    this.clearHeatMapViewParams();
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

  clearHeatMapViewParams() {
    const heatMapProperties = [
      'heatMapCurrentStart',
      'heatMapCurrentEnd',
      'heatMapBaselineStart',
      'heatMapBaselineEnd',
      'heatmapFilters'
      ]
    heatMapProperties.forEach((prop) => {
      this.viewParams[prop] = null
    })
  },

  setupSearchListeners() {

    // $("#analysis-granularity-input").on("select2:selecting", () =>  {
    //   this.searchEvent.notify();
    // });

    $("#analysis-search-button").click(() => {
      if (!this.searchParams.metricId) return;
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
