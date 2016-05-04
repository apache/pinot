function d3Legend() {
  var margin = {top: 5, right: 0, bottom: 5, left: 10},
      width = 400,
      height = 20,
      color = d3.scale.category10().range(),
      dispatch = d3.dispatch('legendClick', 'legendMouseover', 'legendMouseout');


  function chart(selection) {
    selection.each(function(data) {
      /**
      *    Legend curently is setup to automaticaly expand vertically based on a max width.
      *    Should implement legend where EITHER a maxWidth or a maxHeight is defined, then
      *    the other dimension will automatically expand to fit, and anything that exceeds
      *    that will automatically be clipped.
      **/

      var wrap = d3.select(this).selectAll('g.legend').data([data]);
      var gEnter = wrap.enter().append('g').attr('class', 'legend').append('g');


      var g = wrap.select('g')
          .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');


      var series = g.selectAll('.series')
          .data(function(d) { return d });
      var seriesEnter = series.enter().append('g').attr('class', 'series')
          .on('click', function(d, i) {
            dispatch.legendClick(d, i);
          })
          .on('mouseover', function(d, i) {
            dispatch.legendMouseover(d, i);
          })
          .on('mouseout', function(d, i) {
            dispatch.legendMouseout(d, i);
          });
      seriesEnter.append('circle')
          .style('fill', function(d, i){ return d.color || color[i % 10] })
          .style('stroke', function(d, i){ return d.color || color[i % 10] })
          .attr('r', 5);
      seriesEnter.append('text')
          .text(function(d) { return d.label })
          .attr('text-anchor', 'start')
          .attr('dy', '.32em')
          .attr('dx', '8');
      series.classed('disabled', function(d) { return d.disabled });
      series.exit().remove();


      var ypos = 5,
          newxpos = 5,
          maxwidth = 0,
          xpos;
      series
          .attr('transform', function(d, i) {
             var length = d3.select(this).select('text').node().getComputedTextLength() + 28;
             xpos = newxpos;

             //TODO: 1) Make sure dot + text of every series fits horizontally, or clip text to fix
             //      2) Consider making columns in line so dots line up
             //         --all labels same width? or just all in the same column?
             //         --optional, or forced always?
             if (width < margin.left + margin.right + xpos + length) {
               newxpos = xpos = 5;
               ypos += 20;
             }

             newxpos += length;
             if (newxpos > maxwidth) maxwidth = newxpos;

             return 'translate(' + xpos + ',' + ypos + ')';
          });

      //position legend as far right as possible within the total width
      g.attr('transform', 'translate(' + (width - margin.right - maxwidth) + ',' + margin.top + ')');

      height = margin.top + margin.bottom + ypos + 15;
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

  return chart;
}