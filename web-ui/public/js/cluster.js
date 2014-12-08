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

var es_client = null;

function initClusterView() {
  if(!inviso.cluster.initialized) {
    inviso.cluster.selected.subscribe(function() {
      $('#capacity-dateline').dateline('reset')
      loadSelectedCluster();
    });

    inviso.cluster.initialized = true;
  }



  var $clusterSelect = $('#cluster-select');
  $clusterSelect.selectpicker();
  $('#group-by').selectpicker();
  $('#metric').selectpicker();

  initElasticSearch(function(hosts) {
    es_client = new $.es.Client({
      apiVersion: "1.1",
      hosts: hosts,
    });

    getClusters();
  });

  $clusterSelect.on('change', function() {
    if($(this).val() == inviso.cluster.selected()) {
      inviso.cluster.selected.valueHasMutated();
    } else {
      inviso.cluster.selected($(this).val());
    }
  });

  initCharts();
}

function focusCluster() {
  $('.navbar-nav .active').removeClass('active');
  $('.nav-cluster').addClass('active');
}

function loadSelectedCluster() {
  var $clusterSelect = $('#cluster-select');
  $('#application-stream').spin();
  loadAppData(inviso.cluster.selected());

  $('#capacity-stream').spin();
  loadCapacityStream(inviso.cluster.selected());
}

function getClusters() {
  es_client.search({
    index: settings.cluster.index,
    type: 'metrics',
    size: 0,
    body: {
      query: {
        bool: {
          must: [
            {
              range: {
                timestamp: {
                  gte: moment().utc().subtract(14, 'days').valueOf(),
                  lte: moment().utc().valueOf()
                }
              }
            }
          ]
        }
      },
      aggs: {
        clusters: {
          terms: {
            field: "cluster",
            size: settings.cluster.max
          }
        }
      }
    }
  }).then(
    function(result){
      var $clusterSelect = $('#cluster-select');

      var clusters = _.reduce(result.aggregations.clusters.buckets, function(m,v){m.push(v.key); return m;}, []);

      _.each(clusters.sort(), function(v) {
        $clusterSelect.append($("<option value=\""+v+"\">"+v+"</option>"));
      });

      $clusterSelect.selectpicker('refresh');

      if(clusters.contains(settings.cluster.default)) {
        $clusterSelect.selectpicker('val', settings.cluster.default);
      }else {
        inviso.cluster.selected($clusterSelect.val());
      }
    },
    function(error){
      console.log(error);
    }
  );
}

function initCharts() {
  $('#application-stream').stream({
    offset:'zero',
    tooltip: function(d) {
      var data = _.last(_.filter(_.flatten(d), function(v) { return v.id != null; }));
      data.config = d.config;

      return inviso.cluster.tooltip.app(data);
    }
  });

  $('#capacity-stream').stream({
    offset: 'zero',
    brush: true
  }).bind('streambrush', function(event, data) {
    var start = data.range[0];
    var stop = data.range[1];

    $('#application-stream').spin();
    loadAppData(inviso.cluster.selected(), start, stop);
  });

  $('#capacity-dateline').dateline().bind('datelinebrush', function(event, data){
    var start = +data.range[0];
    var stop = +data.range[1];

    $('#application-stream').stream('clear');

    $('#capacity-stream').spin();
    loadCapacityStream(inviso.cluster.selected(), start, stop);
  });

  $("#highlight").keyup(function(event){
    if(event.keyCode == 13){
      $('#application-stream').stream('highlight', $("#highlight").val());
    }
  });
  $("#filter").keyup(function(event){
    if(event.keyCode == 13){
      $('#application-stream').stream('filter', $("#filter").val());
    }
  });
}

function scrollSearch(type, cluster, includes, start, stop, callback) {
  var items = [];

  console.log(cluster + ': ' + new Date(start) +'('+start+')'+' - ' + new Date(stop));

  es_client.search({
    index: 'inviso-cluster',
    type: type,
    body: {
      query: {
        bool: {
          must: [
            { range: { timestamp: { gte: start, lte: stop }}},
            { term: { 'cluster': cluster }}
          ]
        }
      }
    },
    _sourceInclude: includes,
    size: 10000,
    searcyType: 'scan',
    scroll: '10s'
  }).then(function scroll_all(response) {
    if(response.hits.total > 20000) {
      alert('Too many data points.  Please select a smaller range.');
      callback([]);
      return;
    }

    response.hits.hits.forEach(function (hit) {
      items.push(hit._source);
    });

    if(items.length >= response.hits.total) {
      items.sort(function(a,b) { return a.timestamp - b.timestamp; });

      callback(items);
    } else {
      es_client.scroll({
        scrollId: response._scroll_id,
        scroll: '10s'
      }).then(scroll_all);
    }
  });
}

function loadAppData(cluster, start, stop, includes) {
  start = Math.floor(start || (Date.now() - settings.cluster.applicationLookback));
  stop = Math.ceil(stop || Date.now());
  includes = ['timestamp', 'id', 'cluster', 'cluster.id', 'queue', 'user', 'startedTime', 'runningContainers', 'allocatedVCores', 'allocatedMB'];

  scrollSearch('applications', cluster, includes, start, stop, function(data){
    var periods = _.uniq(_.map(data, function(d) {return d.timestamp;})).sort();
    var periodData = _.groupBy(data, function(app) {return app.timestamp; });

    var groupings = {
      app: function() {
        var apps = _.groupBy(_.filter(data,function(app){return app.id != null;}), function(app) {return app.id; });
        var jobIds = _.map(_.keys(apps),function(v){ return v.replace("application","job");});

        var configMap = {};

        var layers = _.reduce(apps, function(m, app) {
          app.sort(function(a,b){return a.timestamp - b.timestamp;});

          var id = app[0].id;
          var user = app[0].user;
          var queue = app[0].queue;

          _.each(periods, function(p,i) {
            if(app.length <= i || app[i].timestamp > p) {
              app.splice(i,0,{x:p, y:0, id:id, user: user, queue: queue});
            } else {
              app[i].x = p;
              app[i].y = app[i][$('#metric').val()];
            }

            app[i].appId = id;

          });
          configMap[id] = app;

          m.push(app);
          return m;
        }, []);

        //Load the inviso data for apps
        if(!jobIds.isEmpty()) {
          es_client.mget({
            index: settings.search.index,
            type: 'config',
            body: {
              ids: jobIds
            },
            _sourceInclude: [
              'mapred.job.id',
              'mapred.job.start',
              'mapred.job.stop',
              'duration',
              'genie.job.name',
              'genie.job.id',
              'mapreduce.job.maps',
              'mapreduce.job.reduces',
              'mapreduce.map.memory.mb',
              'mapreduce.reduce.memory.mb'
            ]
          }, function(error, response) {
            console.log(response);

            _.each(response.docs, function(doc){
              var id = doc._id.replace('job','application');

              if(_.has(configMap, id)) {
                configMap[id].config = doc._source;
              }
            });
          });
        }

        return layers;
      },
      user: function() {
        var users = _.uniq(_.map(data, function(d) {return d.user;})).sort();

        var layers = _.reduce(periodData, function(m, apps, period) {
          var g = _.groupBy(apps, function(d) {return d.user;});

          var layer = [];

          _.each(users, function(user, i){
            var apps = g[user];
            layer.push({
              id: user,
              user: user,
              apps: apps,
              x: parseInt(period),
              y: apps?d3.sum(g[user], function(d){return d[$('#metric').val()];}):0
            });
          });

          m.push(layer);
          return m;
        }, []);

        return layers.transpose();
      },
      queue: function() {
        var queues = _.uniq(_.map(data, function(d) {return d.queue;})).sort();

        var layers = _.reduce(periodData, function(m, apps, period) {
          var g = _.groupBy(apps, function(d) {return d.queue;});

          var layer = [];

          _.each(queues, function(queue, i){
            var apps = g[queue];
            layer.push({
              id: queue,
              queue: queue,
              apps: apps,
              x: parseInt(period),
              y: apps?d3.sum(g[queue], function(d){return d[$('#metric').val()];}):0
            });
          });

          m.push(layer);
          return m;
        }, []);

        return layers.transpose();
      }
    };

    var $as = $('#application-stream');

    function update() {
      var grouping = $('#group-by').val();
      var metric = $('#metric').val();
      var label = $('.metric-form button').text() + " by " + $('.group-by-form button').text();
      $as.stream('update', {data: groupings[grouping](), yLabel: label});
    }

    $('#group-by, #metric').off('change.inviso').on('change.inviso', function(){
      update();
    });

    update();
    $as.spin(false);
  });
}

function loadCapacityStream(cluster, start, stop) {
  start = start || Date.now() - settings.cluster.capacityLookback;
  stop = stop || Date.now();
  var includes = [ 'timestamp', 'cluster', 'cluster.id', 'appsRunning', 'appsPending' , 'containersPending' , 'containersReserved', 'containersAllocated'];

  scrollSearch('metrics', cluster, includes, start, stop, function(data){
    var clusters = _.uniq(_.map(data, function(m) {return m['cluster.id'];})).sort();
    var periodData = _.groupBy(data, function(m) {return m.timestamp; });

    var layers = _.map(periodData, function(metrics, period){
      var g = _.groupBy(metrics, function(p) {return p['cluster.id'];});

      var layer = [];

      _.each(clusters, function(c) {
        layer.push({
          id: 'allocated-'+c,
          x: parseInt(period),
          y: g[c]?d3.sum(g[c], function(d){return d.containersAllocated;}):0
        });

        layer.push({
          id: 'reserved-'+c,
          x: parseInt(period),
          y: g[c]?d3.sum(g[c], function(d){return d.containersReserved;}):0
        });

        layer.push({
          id: 'pending-'+c,
          x: parseInt(period),
          y: g[c]?d3.sum(g[c], function(d){return d.containersPending;}):0
        });
      });

      return layer;
    });

    var colorRange = d3.scale.linear().range(["#2d4fca", "#90cde7"]);

    legend = [
      {text:"Allocated", color:colorRange(0)},
      {text:"Reserved", color:colorRange(0.5)},
      {text:"Pending", color:colorRange(1)}
    ];

    if(!layers.isEmpty()) {
      layers = layers.transpose();
    }

    for(var i=0; i<layers.length; i++) {
      layers[i].color = colorRange(i/layers.length);
    }

    var $cs = $('#capacity-stream');
    $cs.stream('update', { data: layers, yLabel: "Containers", legend: legend});
    $cs.spin(false);
  });
}
