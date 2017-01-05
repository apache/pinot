function DimensionTreeMapView(dimensionTreeMapModel) {
  var template = $("#dimension-tree-map-template").html();
  this.template_compiled = Handlebars.compile(template);
  this.placeHolderId = "#dimension-tree-map-placeholder";
  this.dimensionTreeMapModel = dimensionTreeMapModel;
}

DimensionTreeMapView.prototype = {
  render: function () {
    if (this.dimensionTreeMapModel.heatmapData) {
      var result = this.template_compiled(this.dimensionTreeMapModel);
      $(this.placeHolderId).html(result);
      this.renderTreemapHeaderSection();
      this.renderTreemapSection();
      this.setupListenersForMode();
    }
  },

  renderTreemapHeaderSection : function() {
    var self = this;
    function current_range_cb(start, end) {
      $('#heatmap-current-range span').addClass("time-range").html(start.format('MMM D, ') + start.format('hh:mm a') + '  &mdash;  ' + end.format('MMM D, ') + end.format('hh:mm a'));
    }
    function baseline_range_cb(start, end) {
      $('#heatmap-baseline-range span').addClass("time-range").html(start.format('MMM D, ') + start.format('hh:mm a') + '  &mdash;  ' + end.format('MMM D, ') + end.format('hh:mm a'));
    }
    this.renderDatePicker('#heatmap-current-range', current_range_cb, self.dimensionTreeMapModel.currentStart, self.dimensionTreeMapModel.currentEnd);
    this.renderDatePicker('#heatmap-baseline-range', baseline_range_cb, self.dimensionTreeMapModel.baselineStart, self.dimensionTreeMapModel.baselineEnd);
    current_range_cb(self.dimensionTreeMapModel.currentStart, self.dimensionTreeMapModel.currentEnd);
    baseline_range_cb(self.dimensionTreeMapModel.baselineStart, self.dimensionTreeMapModel.baselineEnd);
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

  renderTreemapSection : function() {
    // DRAW THE AXIS
    // Create the SVG Viewport
    var height = $('#axis-placeholder').height();
    var width = $('#axis-placeholder').width();
    var lineGraph = d3.select("#axis-placeholder").append("svg:svg").attr("width", width).attr("height", height);
    var myLine = lineGraph.append("svg:line").attr("x1", 0).attr("y1", height - 1).attr("x2", width).attr("y2", height - 1).style("stroke", "rgb(6,120,155)");
    var startValue = 0;
    var endValue = this.dimensionTreeMapModel.currentTotal;
    var middleValue = this.dimensionTreeMapModel.currentTotal/2;
    var startLabel = lineGraph.append("svg:text").attr("x", 0).attr("y", height - 6).text(startValue).attr("font-family", "SourceSansPro").attr("font-size", "20px");
    var endLabel = lineGraph.append("svg:text").attr("x", width / 2).attr("y", height - 6).text(middleValue).attr("font-family", "SourceSansPro").attr("font-size", "20px").attr("text-anchor",
        "middle");
    var endLabel = lineGraph.append("svg:text").attr("x", width).attr("y", height - 6).text(endValue).attr("font-family", "SourceSansPro").attr("font-size", "20px").attr("text-anchor", "end");

    var margin = 0;

    var self = this;

    var getChangeFactor = function (dataRow) {
      var factor = dataRow.percentageChange;
      if (self.dimensionTreeMapModel.heatmapMode === 'contributionChange') {
        factor = dataRow.contributionChange;
      }
      if (self.dimensionTreeMapModel.heatmapMode === 'contributionToOverallChange') {
        factor = dataRow.contributionToOverallChange;
      }
      return factor;
    };

    var getBackgroundColor = function (factor) {
      var opacity = Math.abs(factor / 25);
      if(factor > 0){
        return "rgba(0,0,234," + opacity + ")";
      } else{
        return "rgba(234,0,0,"  + opacity + ")" ;
      }
    };

    var getTextColor = function (factor) {
      var opacity = Math.abs(factor / 25);
      if(opacity < 0.5){
        return "#000000";
      } else{
        return "#ffffff" ;
      }
    };

    var dimensions = this.dimensionTreeMapModel.dimensions;
    for (var i = 0; i < dimensions.length; i++) {
      var data = this.dimensionTreeMapModel.treeMapData[i];
      var dimension = dimensions[i];
      var dimensionPlaceHolderId = '#' + dimension + '-heatmap-placeholder';
      var height = $(dimensionPlaceHolderId).height();
      var width = $(dimensionPlaceHolderId).width();
      var treeMap = d3.layout.treemap().mode("slice").size([ width, height ]).sort(function(a, b) {
        return a.value - b.value;
      });

      var div = d3.select(dimensionPlaceHolderId).attr("class", "heatmap")
          .append("svg:svg").attr("width", width).attr("height", height).append("svg:g").attr("transform", "translate(.5,.5)");

      var nodes = treeMap.nodes(data).filter(function(d) {
        return !d.children;
      });
      var cell = div.selectAll("g").data(nodes).enter().append("svg:g").attr("class", "cell").attr("transform", function(d) {
        return "translate(" + d.x + "," + d.y + ")";
      }).on("click", function(d) {
        // return zoom(node == d.parent ? root : d.parent);
      }).on("mousemove", function(d) {
        var xPosition = d3.event.pageX + 5;
        var yPosition = d3.event.pageY + 5;
        d3.select("#tooltip")
          .style("left", xPosition + "px")
          .style("top", yPosition + "px");
        d3.select("#tooltip #heading")
          .text(d.t);
        d3.select("#tooltip #percentageChange")
          .text(d.percentageChange);
        d3.select("#tooltip #currentValue")
          .text(d.value);
        d3.select("#tooltip").classed("hidden", false);
      }).on("mouseout", function() {
        d3.select("#tooltip").classed("hidden", true);
      });

      cell.append("svg:rect").attr("width", function(d) {
        return d.dx - 1
      }).attr("height", function(d) {
        return d.dy - 1
      }).style("fill", function(d) {
        var factor = getChangeFactor(d);
        return getBackgroundColor(factor);
      });

      cell.append("svg:text").attr("x", function(d) {
        return d.dx / 2;
      }).attr("y", function(d) {
        return d.dy / 2;
      }).attr("dy", ".35em").attr("text-anchor", "middle").text(function(d) {
        // TODO : add a condition here based on that show percentage change or contribution
        var factor = getChangeFactor(d);
        var text = d.t + '(' + factor + ')';

        //each character takes up 7 pixels on an average
        var estimatedTextLength = text.length * 7;
        if(estimatedTextLength > d.dx) {
          return text.substring(0, d.dx/7) + "..";
        } else {
          return text;
        }
      }).style("opacity", function(d) {
        d.w = this.getComputedTextLength();
        //uncomment this code to detect the average number of pixels per character. Currently its 6 but we use 7 to estimate the text length.
//        if(d.dx < d.w) {
//          text = d.t + '(' + d.percentageChange + ')'  ;
//          //console.log("text-length:"+ d.w + " cell-width:"+ d.dx + " text:" + text + " length:" + text.length + " pixels-per-letter:" + (d.w/text.length))
//        }
        return d.dx > d.w ? 1 : 0;
      }).style("fill", function(d){
        var factor = getChangeFactor(d);
        return getTextColor(factor);
      });

    }
  },

  setupListenersForMode : function () {
    var self = this;
    $("#percent_change a").click(self, function() {
      self.dimensionTreeMapModel.heatmapMode = "percentChange";
      self.render();
    });
    $("#change_in_contribution").click(self, function() {
      self.dimensionTreeMapModel.heatmapMode = "contributionChange";
      self.render();
    });
    $("#contribution_to_overall_change").click(self, function() {
      self.dimensionTreeMapModel.heatmapMode = "contributionToOverallChange";
      self.render();
    });
  },
}


