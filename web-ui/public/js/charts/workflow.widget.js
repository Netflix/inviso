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
 
(function($, window, document, undefined) {

  $.widget("inviso.workflow", {

    options: {
      data: {},
      margin: { left: 125, right: 125, top: 5, bottom: 30 },
      baseHeight: 45,
      barHeight: 20,
      barSeparation: 2,
      ticks: 5,
      color: d3.scale.linear().range(["#99adfd", "#001382"])
    },

    _create: function() {
      var self = this;
      var widget = this;

      var m = widget.options.margin;
      var w = $(widget.element).width();
      var h = this.options.baseHeight + this.options.barHeight;

      var svg = widget.svg = d3.select($(widget.element)[0]).append("svg")
        .attr("width", w)
        .attr("height", h);

      var svgg = widget.svgg = svg.append("g")
        .attr("transform", "translate(" + m.left + "," + m.top + ")");

      now = new Date();
      before = new Date(new Date().setDate(new Date().getDate() - 1));

      widget.x = d3.scale.linear()
        .domain([before.getTime(), now.getTime()])
        .range([0, w - m.right - m.left]);

      widget.xAxis = d3.svg.axis()
        .scale(widget.x)
        .tickFormat(function(d) {
          return moment(d).format('YYYY-MM-DD HH:mm:ss (UTC ZZ)');
        })
        .orient("bottom");

      svgg.append('g')
        .attr("class", "x axis")
        .attr("transform", "translate(0," + (h - m.top - m.bottom) + ")")
        .call(widget.xAxis);

      d3.select(window).on('resize', function() {
        $(widget.element).workflow('update');
      });

      this.update();
    },

    _destroy: function() {

    },

    update: function() {
      this._updateScale();
      this._updateData();
    },

    _updateScale: function() {
      var jobs = this.options.data.jobs();

      var start = Number.POSITIVE_INFINITY;
      var stop = Number.NEGATIVE_INFINITY;

      $.each(jobs, function(i, job) {
        start = Math.min(job.data.start, start);
        stop = Math.max(job.data.stop, stop);
      });

      this.x.domain([start, stop]);

      this.xAxis.scale(this.x);
      this.xAxis.tickValues(this._calculateTicks(start, stop));
      this.svg.select('.x.axis').call(this.xAxis);
    },

    _calculateTicks: function(start, stop) {
      var spacing = (stop - start) / (this.options.ticks - 1);

      var ticks = [];
      ticks.push(start);

      for(var i = 1; i < this.options.ticks - 1; i++) {
        ticks.push(Math.floor(i * spacing + start));
      }

      ticks.push(stop);

      return ticks;
    },

    _updateData: function() {
      var self = this;
      var svgg = this.svgg;
      var m = this.options.margin;

      var overlap = this._getOverlaps();

      //Update graph height
      var lanes = 1;

      $.each(overlap, function(key, value) {
        lanes = Math.max(lanes, value + 1);
      });

      this.svg.attr("height", this.options.baseHeight + (lanes * (this.options.barHeight + this.options.barSeparation)));
      this.svg.select('.x.axis').attr("transform", "translate(0," + (this.svg.attr('height') - m.top - m.bottom) + ")");


      var selection = svgg.selectAll('rect').data(this.options.data.jobs().clone(), function(d) {
        return d.id;
      });

      //Update
      var update = selection;

      update.attr("x", function(d) {
        return self.x(d.data.start);
      })
        .attr("width", function(d) {
          var xStart = self.x(d.data.start);
          var xStop = self.x(d.data.stop);

          return Math.max(xStop - xStart, 1);
        })
        .attr("y", function(d) {
          return overlap[d.id] * self.options.barSeparation + overlap[d.id] * self.options.barHeight;
        });

      //Enter
      var enter = selection.enter();

      enter.append('rect')
        .attr("class", "job")
        .attr("x", function(d) {
          return self.x(d.data.start);
        })
        .attr("rx", 5)
        .attr("width", function(d) {
          var xStart = self.x(d.data.start);
          var xStop = self.x(d.data.stop);

          return Math.max(xStop - xStart, 1);
        })
        .attr("y", function(d) {
          return overlap[d.id] * self.options.barSeparation + overlap[d.id] * self.options.barHeight;
        })
        .attr("ry", 5)
        .attr("height", self.options.barHeight)
        .attr("fill", function(d) {
          var s = d.id.split("_");

          if(d.data.status == "FAILED") {
            return "red";
          } else if(d.data.status == "RUNNING") {
            return "green";
          }

          return self.options.color(Math.random());
        })
        .on("mouseover", function(d) {
          if(stickyTooltip == null) {
            self._tooltip(this, d);
            $(this).tipsy('show');
          }

          var $target = $('#' + d.id);
          $target.css("background-color", "gray");

          var $parent = $target.parent();

          $parent.animate({ scrollLeft: $target.offset().left - $parent.offset().left + $parent.scrollLeft() - $(window).width() / 2 + $target.width() / 2});
        })
        .on("mouseout", function(d) {
          if(stickyTooltip == null) {
            $(this).tipsy('hide');
          }

          var $target = $('#' + d.id);
          $target.css("background-color", "");
        })
        .on("click", function(d) {
          if(stickyTooltip == null) {
            $(this).tipsy('show');
            stickyTooltip = this;

            //Keep clicks from removing
            d3.selectAll('.tipsy *').on('click', function() {
              d3.event.stopPropagation();
            });
          } else {
            $(stickyTooltip).tipsy('hide');
            stickyTooltip = null;
          }

          d3.event.stopPropagation();
        })
      ;

      selection.exit().remove();

    },

    _tooltip: function(target, job) {
      //tooltips
      $(target).tipsy({
        trigger: 'manual',
        gravity: function() {
          if(target.pageX < $(window).width() / 6) {
            return 'nw';
          } else if(target.pageX > $(window).width() * 5 / 6) {
            return 'ne';
          } else {
            return 'n';
          }
        },
        html: true,
        offset: 20,
        opacity: 1,
        title: function() {
          return job.app.tooltip(job);
        }
      });
    },

    _getOverlaps: function() {
      var jobs = this.options.data.jobs().clone().sort(function(l, r) {
        return l.data.start - r.data.start;
      });

      var lanes = [
        []
      ];
      var overlap = {};

      for(var i = 0; i < jobs.length; i++) {
        var j1 = jobs[i];

        for(var j = 0; j < lanes.length; j++) {
          var lane = lanes[j];

          if(_.isEmpty(lane)) {
            lane.push(j1);
            overlap[j1.id] = j;
            break;
          }

          var overlaps = false;

          for(var k = 0; k < lane.length; k++) {
            var j2 = lane[k];

            if(!(j1.data.start > j2.data.stop || j1.data.stop < j2.data.start)) {
              overlaps = true;
              break;
            }
          }

          if(!overlaps) {
            lane.push(j1);
            overlap[j1.id] = j;
            break;
          } else {
            lanes.push([]);
          }
        }
      }

      return overlap;
    }

  });

})(jQuery, window, document);
