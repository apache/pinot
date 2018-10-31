function AnalysisView(analysisModel) {
  // Compile template
  const analysis_template = $("#analysis-template").html();
  this.analysis_template_compiled = Handlebars.compile(analysis_template);

  this.analysisModel = analysisModel;
  this.applyDataChangeEvent = new Event(this);
  this.searchEvent = new Event(this);
  this.viewParams = {};
  this.searchParams = {}
  this.compareMode = 'WoW';

  this.currentRange = () => {
    const maxTime = this.analysisModel.maxTime;
    return {
      'Last 24 Hours': [moment().subtract(24, 'hours').startOf('hour'),
        maxTime.clone()],
      'Yesterday': [moment().subtract(1, 'days').startOf('day'), moment().subtract(1, 'days').endOf('day')],
      'Last 7 Days': [moment().subtract(6, 'days').startOf('day'), maxTime.clone()],
      'Last 30 Days': [moment().subtract(29, 'days').startOf('day'), maxTime.clone()],
      'This Month': [moment().startOf('month'), maxTime.clone()],
      'Last Month': [moment().subtract(1, 'month').startOf('month'),
        moment().subtract(1, 'month').endOf('month')]
    };
  };

  this.baselineRange = () => {
    const maxTime = this.analysisModel.maxTime;
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

  resetViewParams() {
    this.viewParams = {};
  },

  calculateBaselineDate(dateType, offset) {
    const baseDate = this.viewParams[dateType] || moment();
    return baseDate.clone().subtract(offset, 'days');
  },

  getDateRangeFormat(granularity) {
    switch(granularity) {
      case 'DAYS':
        return constants.DATE_RANGE_FORMAT;
      case 'HOURS':
        return constants.DATE_HOUR_RANGE_FORMAT;
      case '5_MINUTES':
        return  constants.DATE_TIME_RANGE_FORMAT;
    }
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
    this.resetViewParams();
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
    const showTime = this.viewParams.granularity !== constants.GRANULARITY_DAY;
    const analysis_options_template = $('#analysis-options-template').html();
    const compiled_analysis_options_template = Handlebars.compile(analysis_options_template);
    const analysisOptionsHTML = compiled_analysis_options_template(this.analysisModel);
    $('#analysis-options-placeholder').html(analysisOptionsHTML);

    this.renderGranularity(metricId);
    this.renderDateRangePickers(showTime);
    this.renderDimensions(metricId);
    this.renderFilters(metricId);
    this.setupApplyListener(metricId);

    // TODO remove hack. use separate root cause view
    if (this.analysisModel.rootCauseEnabled) {
      const rootcause_table_template = $('#rootcause-table-template').html();
      const compiled_rootcause_table_template = Handlebars.compile(rootcause_table_template);
      const rootCauseHTML = compiled_rootcause_table_template(this.analysisModel);
      $('#rootcause-table-placeholder').html(rootCauseHTML);
      $('#rootcause-data-table').DataTable({
        data: this.analysisModel.rootCauseData,
        columns: [
          {
            title: 'Type',
            data: null,
            render: function(data, type, row) { return "<a href=\"" + row.link + "\" target=\"_blank\">" + row.type + "</a>" }
          },
          {
            title: 'Label',
            data: 'label'
          },
          {
            title: 'Score',
            data: 'score'
          }
        ],
        order: [[ 2, 'desc' ], [ 0, 'asc' ], [ 1, 'asc' ]],
        iDisplayLength: 50
      });
    }
  },

  renderDateRangePickers(showTime) {
    showTime = showTime || this.viewParams.granularity === constants.DATE_RANGE_CUSTOM;
    const $currentRangeText = $('#current-range span');
    const $baselineRangeText = $('#baseline-range span');
    const $currentRange = $('#current-range');
    const $baselineRange = $('#baseline-range');
    // TIME RANGE SELECTION
    const currentStart = this.viewParams.currentStart || this.analysisModel.currentStart;
    const currentEnd = this.viewParams.currentEnd || this.analysisModel.currentEnd;
    const baselineStart = this.viewParams.baselineStart || this.analysisModel.baselineStart;
    const baselineEnd = this.viewParams.baselineEnd || this.analysisModel.baselineEnd;

    const setBaselineRange = (start, end, compareMode = constants.DEFAULT_COMPARE_MODE) => {
      const dateFormat = this.getDateRangeFormat(this.viewParams.granularity);
      end = end.isBefore(this.analysisModel.maxTime) ? end : this.analysisModel.maxTime.clone();

      this.viewParams['baselineStart'] = start;
      this.viewParams['baselineEnd'] = end;
      this.viewParams['compareMode'] = compareMode;

      $baselineRangeText.addClass("time-range").html(
        `<span class="time-range__type">${compareMode}</span> ${start.format(dateFormat)} &mdash; ${end.format(dateFormat)}`);
    };
    const setCurrentRange = (start, end, rangeType = constants.DATE_RANGE_CUSTOM) => {
      const $baselineRangePicker = $('#baseline-range');
      const dateFormat = this.getDateRangeFormat(this.viewParams.granularity);
      end = end.isBefore(this.analysisModel.maxTime) ? end : this.analysisModel.maxTime.clone();

      this.viewParams['currentStart'] = start;
      this.viewParams['currentEnd'] = end;

      const compareMode = this.viewParams['compareMode'] || this.compareMode;

      if (compareMode !== constants.DATE_RANGE_CUSTOM) {
        $baselineRangePicker.length && $baselineRangePicker.data('daterangepicker').remove();
        const offset = constants.WOW_MAPPING[compareMode];
        const baselineStart = start.clone().subtract(offset, 'days');
        const baselineEnd = end.clone().subtract(offset, 'days');
        this.renderDatePicker($baselineRange, setBaselineRange, baselineStart, baselineEnd, showTime, this.baselineRange, this.analysisModel.maxTime);
        setBaselineRange(baselineStart, baselineEnd, compareMode);
      }

      $currentRangeText.addClass("time-range").html(
          `<span class="time-range__type">${rangeType}</span> ${start.format(dateFormat)} &mdash; ${end.format(dateFormat)}`)
    };

    this.renderDatePicker($currentRange, setCurrentRange, currentStart, currentEnd, showTime, this.currentRange, this.analysisModel.maxTime);
    this.renderDatePicker($baselineRange, setBaselineRange, baselineStart, baselineEnd, showTime, this.baselineRange, this.analysisModel.maxTime);

    const currentDatePicker = $currentRange.data('daterangepicker')
    currentDatePicker.updateView();
    const currentRangeType = currentDatePicker.chosenLabel;

    setCurrentRange(currentStart, currentEnd, currentRangeType);
  },

  renderDatePicker($selector, callbackFun, initialStart, initialEnd, showTime, rangeGenerator, maxTime) {
    const ranges = rangeGenerator();
    $selector.daterangepicker({
      startDate: initialStart,
      endDate: initialEnd,
      maxDate: maxTime,
      dateLimit: {
      months: 6
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
    }
    if (paramGranularity && granularities.includes(paramGranularity)) {
      $granularitySelector.val(paramGranularity).trigger('change');
    }

    $granularitySelector.on('change', (event) => {
      const granularity = $(event.currentTarget).select2('data')[0].id;
      const showTime = granularity !== constants.GRANULARITY_DAY;
      this.viewParams['granularity'] = granularity;
      this.destroyDatePickers();
      this.renderDateRangePickers(showTime);
    });
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
        children.push({id:key +"::"+ values[i], text:values[i]});
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
              paramFilters.push(`${key}::${filterName}`);
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
      var keyVal = filterStr.split("::");
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
    this.viewParams['heatMapFilters'] = filterMap;
  },

  clearHeatMapViewParams() {
    const heatMapProperties = [
      'heatMapCurrentStart',
      'heatMapCurrentEnd',
      'heatMapBaselineStart',
      'heatMapBaselineEnd',
      'heatMapFilters'
      ]
    heatMapProperties.forEach((prop) => {
      this.viewParams[prop] = null
    })
  },

  setupSearchListeners() {
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
