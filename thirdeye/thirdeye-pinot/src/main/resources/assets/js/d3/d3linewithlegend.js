function d3LineWithLegend() {
  var margin = {top: 30, right: 10, bottom: 40, left: 60},
      width = 960,
      height = 500,
      dotRadius = function() { return 2.5 },
      xAxisLabelText = false,
      yAxisLabelText = false,
      color = d3.scale.category10().range(),
      dispatch = d3.dispatch('showTooltip', 'hideTooltip');

  var x = d3.scale.linear(),
      y = d3.scale.linear(),
      xAxis = d3.svg.axis().scale(x).orient('bottom'),
      yAxis = d3.svg.axis().scale(y).orient('left'),
      legend = d3Legend().height(30).color(color),
      lines = d3Line();


  function chart(selection) {
    selection.each(function(data) {
      var series = data.filter(function(d) { return !d.disabled })
            .map(function(d) { return d.data });

      x   .domain(d3.extent(d3.merge(series), function(d) { return d[0] } ))
          .range([0, width - margin.left - margin.right]);

      y   .domain(d3.extent(d3.merge(series), function(d) { return d[1] } ))
          .range([height - margin.top - margin.bottom, 0]);

      lines
        .width(width - margin.left - margin.right)
        .height(height - margin.top - margin.bottom)
        .color(data.map(function(d,i) {
          return d.color || color[i % 10];
        }).filter(function(d,i) { return !data[i].disabled }))

      xAxis
        .ticks( width / 100 )
        .tickSize(-(height - margin.top - margin.bottom), 0);
      yAxis
        .ticks( height / 36 )
        .tickSize(-(width - margin.right - margin.left), 0);


      var wrap = d3.select(this).selectAll('g.wrap').data([data]);
      var gEnter = wrap.enter().append('g').attr('class', 'wrap d3lineWithLegend').append('g');

      gEnter.append('g').attr('class', 'legendWrap');
      gEnter.append('g').attr('class', 'x axis');
      gEnter.append('g').attr('class', 'y axis');
      gEnter.append('g').attr('class', 'linesWrap');


      legend.dispatch.on('legendClick', function(d, i) { 
        d.disabled = !d.disabled;

        if (!data.filter(function(d) { return !d.disabled }).length) {
          data.forEach(function(d) {
            d.disabled = false;
          });
        }

        selection.transition().call(chart)
      });


      legend.dispatch.on('legendMouseover', function(d, i) {
        d.hover = true;
        selection.transition().call(chart)
      });

      legend.dispatch.on('legendMouseout', function(d, i) {
        d.hover = false;
        selection.transition().call(chart)
      });


      lines.dispatch.on('pointMouseover.tooltip', function(e) {
        dispatch.showTooltip({
          point: e.point,
          series: e.series,
          pos: [e.pos[0] + margin.left, e.pos[1] + margin.top],
          seriesIndex: e.seriesIndex,
          pointIndex: e.pointIndex
        });
      });

      lines.dispatch.on('pointMouseout.tooltip', function(e) {
        dispatch.hideTooltip(e);
      });




      legend
          .color(color)
          .width(width / 2 - margin.right);

      wrap.select('.legendWrap')
          .datum(data)
          .attr('transform', 'translate(' + (width/2 - margin.left) + ',' + (-legend.height()) +')')
          .call(legend);


      //TODO: maybe margins should be adjusted based on what components are used: axes, axis labels, legend
      margin.top = legend.height();  //need to re-render to see update

      var g = wrap.select('g')
          .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');



      var linesWrap = wrap.select('.linesWrap')
          .datum(data.filter(function(d) { return !d.disabled }));

      d3.transition(linesWrap).call(lines);



      var xAxisLabel = g.select('.x.axis').selectAll('text.axislabel')
          .data([xAxisLabelText || null]);
      xAxisLabel.enter().append('text').attr('class', 'axislabel')
          .attr('text-anchor', 'middle')
          .attr('x', x.range()[1] / 2)
          .attr('y', margin.bottom - 20);
      xAxisLabel.exit().remove();
      xAxisLabel.text(function(d) { return d });

      g.select('.x.axis')
          .attr('transform', 'translate(0,' + y.range()[0] + ')')
          .call(xAxis)
        .selectAll('line.tick')
        .filter(function(d) { return !d })
          .classed('zero', true);


      var yAxisLabel = g.select('.y.axis').selectAll('text.axislabel')
          .data([yAxisLabelText || null]);
      yAxisLabel.enter().append('text').attr('class', 'axislabel')
          .attr('transform', 'rotate(-90)')
          .attr('text-anchor', 'middle')
          .attr('y', 20 - margin.left);
      yAxisLabel.exit().remove();
      yAxisLabel
          .attr('x', -y.range()[0] / 2)
          .text(function(d) { return d });

      g.select('.y.axis')
          .call(yAxis)
        .selectAll('line.tick')
        .filter(function(d) { return !d })
          .classed('zero', true);
    });

    return chart;
  }

  chart.dispatch = dispatch;

  chart.margin = function(_) {
    if (!arguments.length) return margin;
    margin = _;
    return chart;
  };

  chart.width = function(_) {
    if (!arguments.length) return width;
    width = _;
    return chart;
  };

  chart.height = function(_) {
    if (!arguments.length) return height;
    height = _;
    return chart;
  };

  chart.color = function(_) {
    if (!arguments.length) return color;
    color = _;
    return chart;
  };

  chart.dotRadius = function(_) {
    if (!arguments.length) return dotRadius;
    dotRadius = d3.functor(_);
    lines.dotRadius = d3.functor(_);
    return chart;
  };

  //TODO: consider directly exposing both axes
  //chart.xAxis = xAxis;

  //Expose the x-axis' tickFormat method.
  chart.xAxis = {};
  d3.rebind(chart.xAxis, xAxis, 'tickFormat');

  chart.xAxis.label = function(_) {
    if (!arguments.length) return xAxisLabelText;
    xAxisLabelText = _;
    return chart;
  }

  // Expose the y-axis' tickFormat method.
  //chart.yAxis = yAxis;

  chart.yAxis = {};
  d3.rebind(chart.yAxis, yAxis, 'tickFormat');

  chart.yAxis.label = function(_) {
    if (!arguments.length) return yAxisLabelText;
    yAxisLabelText = _;
    return chart;
  }

  return chart;
}