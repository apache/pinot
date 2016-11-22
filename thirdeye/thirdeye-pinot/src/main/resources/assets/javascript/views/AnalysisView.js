function AnalysisView() {

}

AnalysisView.prototype = {

  init : function() {

  },

  render : function() {
    var result_analysis_template_compiled = analysis_template_compiled({});
    $("#analysis-place-holder").html(result_analysis_template_compiled);
    renderAnalysisTab();
  }
}

function renderAnalysisTab() {
  // TIME RANGE SELECTION
  var current_start = moment().subtract(1, 'days');
  var current_end = moment();

  var baseline_start = moment().subtract(6, 'days');
  var baseline_end = moment().subtract(6, 'days');

  function current_range_cb(start, end) {
    $('#current-range span').addClass("time-range").html(start.format('MMM D, ') + start.format('hh:mm a') + '  &mdash;  ' + end.format('MMM D, ') + end.format('hh:mm a'));
  }
  function baseline_range_cb(start, end) {
    $('#baseline-range span').addClass("time-range").html(start.format('MMM D, ') + start.format('hh:mm a') + '  &mdash;  ' + end.format('MMM D, ') + end.format('hh:mm a'));
  }

  $('#current-range').daterangepicker({
    startDate : current_start,
    endDate : current_end,
    dateLimit : {
      days : 60
    },
    showDropdowns : true,
    showWeekNumbers : true,
    timePicker : true,
    timePickerIncrement : 5,
    timePicker12Hour : true,
    ranges : {
      'Last 24 Hours' : [ moment(), moment() ],
      'Yesterday' : [ moment().subtract(1, 'days'), moment().subtract(1, 'days') ],
      'Last 7 Days' : [ moment().subtract(6, 'days'), moment() ],
      'Last 30 Days' : [ moment().subtract(29, 'days'), moment() ],
      'This Month' : [ moment().startOf('month'), moment().endOf('month') ],
      'Last Month' : [ moment().subtract(1, 'month').startOf('month'), moment().subtract(1, 'month').endOf('month') ]
    },
    buttonClasses : [ 'btn', 'btn-sm' ],
    applyClass : 'btn-primary',
    cancelClass : 'btn-default'
  }, current_range_cb);

  $('#baseline-range').daterangepicker({
    startDate : baseline_start,
    endDate : baseline_end,
    dateLimit : {
      days : 60
    },
    showDropdowns : true,
    showWeekNumbers : true,
    timePicker : true,
    timePickerIncrement : 5,
    timePicker12Hour : true,
    ranges : {
      'Last 24 Hours' : [ moment(), moment() ],
      'Yesterday' : [ moment().subtract(1, 'days'), moment().subtract(1, 'days') ],
      'Last 7 Days' : [ moment().subtract(6, 'days'), moment() ],
      'Last 30 Days' : [ moment().subtract(29, 'days'), moment() ],
      'This Month' : [ moment().startOf('month'), moment().endOf('month') ],
      'Last Month' : [ moment().subtract(1, 'month').startOf('month'), moment().subtract(1, 'month').endOf('month') ]
    },
    buttonClasses : [ 'btn', 'btn-sm' ],
    applyClass : 'btn-primary',
    cancelClass : 'btn-default'
  }, baseline_range_cb);

  current_range_cb(current_start, current_end);
  baseline_range_cb(baseline_start, baseline_end);
  renderHeatmapHeaderSection();
  renderHeatmapSection();
}

function renderHeatmapHeaderSection() {
  // TODO: the min/max possible dates here must be limited to the duration we
  // have fetched the w-o-w data for
  var current_start = moment().subtract(1, 'days');
  var current_end = moment();

  var baseline_start = moment().subtract(6, 'days');
  var baseline_end = moment().subtract(6, 'days');

  function current_range_cb(start, end) {
    $('#heatmap-current-range span').addClass("time-range").html(start.format('MMM D, ') + start.format('hh:mm a') + '  &mdash;  ' + end.format('MMM D, ') + end.format('hh:mm a'));
  }
  function baseline_range_cb(start, end) {
    $('#heatmap-baseline-range span').addClass("time-range").html(start.format('MMM D, ') + start.format('hh:mm a') + '  &mdash;  ' + end.format('MMM D, ') + end.format('hh:mm a'));
  }

  $('#heatmap-current-range').daterangepicker({
    startDate : current_start,
    endDate : current_end,
    dateLimit : {
      days : 60
    },
    showDropdowns : true,
    showWeekNumbers : true,
    timePicker : true,
    timePickerIncrement : 5,
    timePicker12Hour : true,
    ranges : {
      'Last 24 Hours' : [ moment(), moment() ],
      'Yesterday' : [ moment().subtract(1, 'days'), moment().subtract(1, 'days') ],
      'Last 7 Days' : [ moment().subtract(6, 'days'), moment() ],
      'Last 30 Days' : [ moment().subtract(29, 'days'), moment() ],
      'This Month' : [ moment().startOf('month'), moment().endOf('month') ],
      'Last Month' : [ moment().subtract(1, 'month').startOf('month'), moment().subtract(1, 'month').endOf('month') ]
    },
    buttonClasses : [ 'btn', 'btn-sm' ],
    applyClass : 'btn-primary',
    cancelClass : 'btn-default'
  }, current_range_cb);

  $('#heatmap-baseline-range').daterangepicker({
    startDate : baseline_start,
    endDate : baseline_end,
    dateLimit : {
      days : 60
    },
    showDropdowns : true,
    showWeekNumbers : true,
    timePicker : true,
    timePickerIncrement : 5,
    timePicker12Hour : true,
    ranges : {
      'Last 24 Hours' : [ moment(), moment() ],
      'Yesterday' : [ moment().subtract(1, 'days'), moment().subtract(1, 'days') ],
      'Last 7 Days' : [ moment().subtract(6, 'days'), moment() ],
      'Last 30 Days' : [ moment().subtract(29, 'days'), moment() ],
      'This Month' : [ moment().startOf('month'), moment().endOf('month') ],
      'Last Month' : [ moment().subtract(1, 'month').startOf('month'), moment().subtract(1, 'month').endOf('month') ]
    },
    buttonClasses : [ 'btn', 'btn-sm' ],
    applyClass : 'btn-primary',
    cancelClass : 'btn-default'
  }, baseline_range_cb);

  current_range_cb(current_start, current_end);
  baseline_range_cb(baseline_start, baseline_end);
}

function renderHeatmapSection() {
  // DRAW THE AXIS
  // Create the SVG Viewport
  var height = $('#axis-placeholder').height();
  var width = $('#axis-placeholder').width();
  // var svgContainer =
  // d3.select("#axis-placeholder").append("svg").attr("width",
  // width).attr("height", height);
  var lineGraph = d3.select("#axis-placeholder").append("svg:svg").attr("width", width).attr("height", height);
  var myLine = lineGraph.append("svg:line").attr("x1", 0).attr("y1", height - 1).attr("x2", width).attr("y2", height - 1).style("stroke", "rgb(6,120,155)");
  startValue = "0";
  middleValue = "50,000";
  endValue = "100,000";
  var startLabel = lineGraph.append("svg:text").attr("x", 0).attr("y", height - 6).text(startValue).attr("font-family", "SourceSansPro").attr("font-size", "20px");
  var endLabel = lineGraph.append("svg:text").attr("x", width / 2).attr("y", height - 6).text(middleValue).attr("font-family", "SourceSansPro").attr("font-size", "20px").attr("text-anchor", "middle");
  var endLabel = lineGraph.append("svg:text").attr("x", width).attr("y", height - 6).text(endValue).attr("font-family", "SourceSansPro").attr("font-size", "20px").attr("text-anchor", "end");

  margin = 0;

  // data = JSON
  // .parse('{"t":"0","children":[{"t":"00","value":2},{"t":"01","children":[{"t":"010","value":1},{"t":"011","value":1},{"t":"012","value":1},{"t":"013","value":1}]},{"t":"02","children":[{"t":"020","value":2},{"t":"021","value":1},{"t":"022","value":1},{"t":"023","value":1},{"t":"024","value":1},{"t":"025","value":2}]},{"t":"03","children":[{"t":"030","value":2},{"t":"031","value":2},{"t":"032","value":2},{"t":"033","value":2}]},{"t":"04","value":5}]}');
  data = JSON.parse('{"t":"0", "children":[{"t":"010","value":100},{"t":"011","value":50},{"t":"012","value":5},{"t":"013","value":25}]}');
  var height = $('#browser-heatmap-placeholder').height();
  var width = $('#browser-heatmap-placeholder').width();

  var dimensions = [ "browser", "country", "device" ];
  for (i = 0; i < dimensions.length; i++) {
    dimension = dimensions[i];
    // var div = d3.select('#' + dimensions[i] +
    // '-heatmap-placeholder').style('width', width).style('height',
    // height).style('position', 'relative');
    var treeMap = d3.layout.treemap().mode("slice").size([ width, height ]).sort(function(a, b) {
      return a.value - b.value;
    });

    var div = d3.select('#' + dimensions[i] + '-heatmap-placeholder').attr("class", "heatmap")
    // .style("width", width + "px")
    // .style("height", height + "px")
    .append("svg:svg").attr("width", width).attr("height", height).append("svg:g").attr("transform", "translate(.5,.5)");

    var nodes = treeMap.nodes(data).filter(function(d) {
      return !d.children;
    });
    var cell = div.selectAll("g").data(nodes).enter().append("svg:g").attr("class", "cell").attr("transform", function(d) {
      return "translate(" + d.x + "," + d.y + ")";
    }).on("click", function(d) {
      return zoom(node == d.parent ? root : d.parent);
    });

    cell.append("svg:rect").attr("width", function(d) {
      return d.dx - 1
    }).attr("height", function(d) {
      return d.dy - 1
    }).style("fill", function(d) {
      return '#EC9F98'
    });

    cell.append("svg:text").attr("x", function(d) {
      return d.dx / 2;
    }).attr("y", function(d) {
      return d.dy / 2;
    }).attr("dy", ".35em").attr("text-anchor", "middle").text(function(d) {
      return d.t;
    }).style("opacity", function(d) {
      d.w = this.getComputedTextLength();
      return d.dx > d.w ? 1 : 0;
    });

  }
}