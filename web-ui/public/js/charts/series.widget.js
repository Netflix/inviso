/*
 *
 *  Copyright 2014 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

stickyTooltip = null;

$(function(){
  d3.select('body').on('click', function() {
    if(window.stickyTooltip != null) {
      $(stickyTooltip).tipsy('hide');
      stickyTooltip = null;
    }
  });
});


(function ( $, window, document, undefined ) {

  $.widget( "bdp.series", {

    options: {
      margin: {top: 20, right: 40, bottom: 80, left: 80},
      barHeight: 1,
      animateLimit: 2000,
      scaled: false,
      flat: false,
      color: function(d) {return d.color;},
      legend: null,
      showLegend: false,
      xLabel: null,
      yLabel: null
    },

    _create: function () {
      var self = this;
      var $e = $(this.element);
      var m = this.options.margin;

      this.width = $e.width();
      this.height = $e.height();

      this.svg = d3.select(this.element[0]).append("svg")
        .attr("width", this.width)
        .attr("height", this.height);

      this.chart = this.svg.append('g')
        .attr("transform", "translate(" + m.left + "," + m.top + ")");

      // X AXIS
      this.x = d3.scale.linear()
        .range([0, this.width - m.right - m.left])
        .domain([0, 100]);

      this.xAxis = d3.svg.axis()
        .scale(this.x)
        .ticks(5)
        .tickFormat(function(d){ return hhmmss(d); })
        .orient("bottom");

      this.svg.append('g')
        .attr("class","x axis")
        .attr("transform", "translate("+m.left+"," + (this.height - m.bottom) + ")")
        .call(this.xAxis);

      this.svg.selectAll(".x.axis text")
        .attr("transform", function(d) {
          return "translate(10,0) rotate(45) translate(" + this.getBBox().width/2 + "," +
          this.getBBox().height/2 + ")";
        });

      this.xLabel = this.svg.append('g')
        .attr('transform', 'translate('+(this.width-m.right-m.left)/2+','+self.height+')');
      this.xLabel
        .append('text')
        .attr('class', 'x-label')
        .text(this.options.xLabel);

      //Y AXIS
      this.y = d3.scale.linear()
        .range([this.height - m.top - m.bottom, 0])
        .domain([0,this.height]);

      this.yAxis = d3.svg.axis()
        .scale(this.y)
        .orient("left");

      this.svg.append('g')
        .attr("class","y axis")
        .attr("transform", "translate("+m.left+","+m.top+")")
        .call(this.yAxis);

      this.yLabel = this.svg.append('g')
        .attr('transform', 'translate(20,'+(self.height -m.top -m.bottom)/2+') rotate(-90)');
      this.yLabel.append('text')
        .attr('class', 'y-label')
        .text(this.options.yLabel);


      this.legend = this.svg.append('g')
        .attr('class', 'legend');

      if(this.options.series) {
        this.update(this.options.series);
      }
    },

    update: function(series) {
      if(series != null) {
        this.series = series;
      }

      this._updateAxes();
      this._updateLegend();
      this._updateData();
    },

    _updateAxes: function() {
      if (this.series == null) {
          return;
      }

      var self = this;
      var $e = $(this.element);
      var o = this.options;
      var m = this.options.margin;

      var data = this.series.data;
      var entries = this.series.data.entries;
      var height = entries.length * o.barHeight + m.top + m.bottom;

      var maxWidth = parseInt($(this.element).css('max-width'),10);
      var minHeight = parseInt($(this.element).css('min-height'),10);

      if (entries.length < o.height) {
        o.barHeight = Math.min(10, Math.floor(o.height / entries.length));
        height = o.barHeight * entries.length;
      }

      var width = data.duration;

      if(o.flat) {
        width = data.longest;
      }

      this.width = width = Math.min(Math.max(width, $e.width() - m.left - m.right), maxWidth);
      this.height = height = Math.max(height, minHeight);

      this.svg
        .attr("width", this.width)
        .attr("height", this.height);

      $e.width(width);

      //Update x axis
      this.svg.select('.x.axis').attr("transform", "translate("+m.left+"," + (this.height - m.bottom) + ")");
      this.x.domain([0, o.flat?data.longest:data.duration]);
      this.x.range([0, width - m.left - m.right]);
      this.svg.select('.x.axis').call(this.xAxis);

      this.svg.selectAll(".x.axis text")
        .attr("transform", function(d) {
          return "translate(10,0) rotate(45) translate(" + this.getBBox().width/2 + "," +
          this.getBBox().height/2 + ")";
        });

      this.xLabel.select('.x-label').text(o.xLabel || this.series.xLabel || '');
      this.xLabel.attr('transform', 'translate('+((this.width-m.right-m.left)/2 + m.right)+','+self.height+')');

      //Update y axis
      this.yAxis.ticks(Math.max(height/100, 10));
      this.y.domain([Math.max(entries.length, minHeight), 0]);
      this.y.range([Math.max(entries.length, height - m.top - m.bottom), 0]);
      this.svg.select('.y.axis').call(this.yAxis);
      this.yLabel.select('.y-label').text(o.yLabel || this.series.yLabel || '');
    },

    _updateLegend: function() {
      var self = this;
      var o = this.options;
      var legend = o.legend || this.series.legend;
      var $legend = $(this.element).find('.legend');

      $legend.empty();

      if(o.showLegend) {
        $legend.show();
      } else {
        $legend.hide();
      }

      if(legend == null) {
        return;
      }

      var i = 0;
      $.each(legend, function(text, color){
        self.legend.append('rect')
          .attr({
            x: 0,
            y: i * 20,
            width: 10,
            height: 10,
            fill: color
          });
        self.legend.append('text')
          .attr({
            x: 15,
            y: i * 20 + 10
          })
          .text(text);

          i++;
      });


      this.legend.attr('transform', function(d) {
        var xpos = self.width - o.margin.right - this.getBBox().width;
        var ypos = o.margin.top;
        return 'translate('+xpos+','+ypos+')';
      });


    },

    _updateData: function() {
      var self = this;
      var $e = $(this.element);
      var o = this.options;
      var data = this.series.data;
      var entries = this.series.data.entries;
      var chart = this.chart;
      var x = this.x;

      var animate = entries.length < o.animateLimit;

      var selection = chart.selectAll('rect.entry').data(entries, function(d) {
          return d.id;
      });

      //enter
      selection.enter()
        .append('rect')
        .attr({
          class: 'entry',
          x: function(d) { return o.flat? 0 :x(d.start); },
          y: function(d, i) { return i * o.barHeight; },
          width: function(d) { return Math.max(x(d.stop) - x(d.start), 1); },
          height: o.barHeight,
          fill: function(d) { return (o.color!=null)?o.color(d):d.color; }
        })
        // .attr("data-toggle", "tooltip")
        .on("mouseover", function(d) {
          if(stickyTooltip == null) {
            self._tooltip(this, self.series, d);
            $(this).tipsy('show');
          }

          if(!animate) {
              return;
          }

          chart.selectAll("rect.entry").attr("fill-opacity", function(d2) {
            if (d != d2) {
                return 0.25;
            }

            return 1;
          })
          .attr("fill", function(d2) {
              if (d != d2) {
                  return "grey";
              }

              return o.color(d);
          });
        })
        .on("mouseout", function(d) {
            if(stickyTooltip == null) {
                $(this).tipsy('hide');
            }

            if(!animate) {
                return;
            }

            chart.selectAll("rect.entry").attr("fill-opacity", function(d2) {
                return 1;
            }).attr("fill", o.color);
        })
        .on("click", function(d){
            if(stickyTooltip == null) {
                $(this).tipsy('show');
                stickyTooltip = this;

                //Keep clicks from removing
                d3.selectAll('.tipsy *').on('click', function(){
                    d3.event.stopPropagation();
                });
            } else {
                $(stickyTooltip).tipsy('hide');
                stickyTooltip = null;
            }

            d3.event.stopPropagation();
        });

      selection
        .attr({
          x: function(d) { return o.flat? 0 :x(d.start); },
          y: function(d, i) { return i * o.barHeight; },
          width: function(d) { return Math.max(x(d.stop) - x(d.start), 1); },
          height: o.barHeight,
          fill: o.color
        });

      //exit
      selection.exit().remove();
    },

    _tooltip: function(elem, series, d) {
      $(elem).tipsy({
        trigger: 'manual',
        gravity: function(){
            if (window.event.pageX < $(window).width()/2) {
                return 'w';
            } else {
                return 'e';
            }
        },
        html: true,
        offset: 10,
        opacity: 1,
        title: function() { return series.app.tooltip(series, d); }
      });
    },

    _setOption: function( key, value ) {
      this._super( key, value );

      if(key === 'showLegend') {
        this._updateLegend();
      } else {
        this.update();
      }
    },

    _setOptions: function( options ) {
      this._super( options );
    }
  });



})( jQuery, window, document );
