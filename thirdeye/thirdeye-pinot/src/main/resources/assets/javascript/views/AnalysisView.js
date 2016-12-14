function AnalysisView(analysisModel) {
  // Compile template
  var analysis_template = $("#analysis-template").html();
  this.analysis_template_compiled = Handlebars.compile(analysis_template);
  this.analysisModel = analysisModel;
  this.applyDataChangeEvent = new Event(this);
  this.viewParams = {granularity: "DAYS"};
}

AnalysisView.prototype = {
  init: function () {
  },

  render: function () {
    $("#analysis-place-holder").html(this.analysis_template_compiled);
    var self = this;
    // METRIC SELECTION
    $('#analysis-metric-input').select2({
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
              id: item.id, text: item.alias
            });
          });
          return {
            results: results
          };
        }
      }
    }).on("select2:select", function (e) {
      console.log(e);
      var selectedElement = $(e.currentTarget);
      var selectedData = selectedElement.select2("data");
      var metricId = selectedData.map(function (e) {
        return e.id
      })[0];
      var metricAlias = selectedData.map(function (e) {
        return e.text
      })[0];
      self.viewParams['metric'] = {id: metricId, alias: metricAlias};
    });

    // TIME RANGE SELECTION
    var current_start = self.analysisModel.currentStart
    var current_end = self.analysisModel.currentEnd;
    var baseline_start = self.analysisModel.baselineStart;
    var baseline_end = self.analysisModel.baselineEnd;

    current_range_cb(current_start, current_end);
    baseline_range_cb(baseline_start, baseline_end);

    $('#current-range').daterangepicker({
      startDate: current_start,
      endDate: current_end,
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
    }, current_range_cb);

    $('#baseline-range').daterangepicker({
      startDate: baseline_start,
      endDate: baseline_end,
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
    }, baseline_range_cb);

    function current_range_cb(start, end) {
      console.log(self.viewParams);
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

  setupListeners: function () {
    self = this;
    $("#analysis-apply-button").click(function (e) {
      console.log(e);
      self.applyDataChangeEvent.notify(self.viewParams);
    });
  },

  renderDimensions: function () {
    // TODO : Populate this on metricSelect
  },

  renderFilters: function () {
    // TODO : populate this on metric select
  }
};
