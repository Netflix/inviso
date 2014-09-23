##
#
#  Copyright 2014 Netflix, Inc.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
#
##

colors=
  MAP: 'blue'
  REDUCE: 'orange'
  RUNNING: 'green'
  KILLED: 'lightgray'
  FAILED: 'red'

localityColors=
  NODE_LOCAL: 'green'
  RACK_LOCAL: 'orange'
  OFF_SWITCH: 'red'

statusMap=
  RUNNING:    { text: 'RUNNING', class: 'label label-success'}
  INITED:    { text: 'SUCCEEDED', class: 'label label-default'}
  SUCCEEDED: { text: 'SUCCEEDED', class: 'label label-default'}
  SUCCESS: { text: 'SUCCEEDED', class: 'label label-default'}
  KILLED: { text: 'KILLED', class: 'label label-warning'}
  FAILED: { text: 'FAILED', class: 'label label-danger'}
  undefined: { text: 'UNKNOWN', class: 'label label-default'}

class MapReduceBase extends Application
  constructor: (version) ->
    @version = version
    @template =
      tooltip: {}

    self = this

    Handlebars.registerHelper('hhmmss', (start, stop) -> hhmmss(stop - start))
    Handlebars.registerHelper('logLink', (job, entry) ->
      return "<a class=\"log-link\" href=\"#{self.logLink(job, entry)}\" target=\"_blank\">Open Logs</a>"
    )
    Handlebars.registerHelper('properties', (attempt) ->
      tbody = ''

      for k,v of attempt
        if k == 'counters' or k == 'entries'
          continue

        if k.toLowerCase().contains('time') or k.toLowerCase().contains('finished')
          v = moment(v).format('YYYY-MM-DD HH:mm:ss (UTC ZZ)')

        tbody += "<tr><td>#{k.replace('_',' ')}</td><td>#{v}</td></tr>"

      return tbody
    )
    Handlebars.registerHelper('counters', (counters) ->
      tbody = ''

      for gname, group of counters
        tbody += "<tr class=\"counter-group\"><td>#{gname}</td><td></td></tr>"
        for cname in _.keys(group).sort()
          tbody += "<tr><td>#{cname}</td><td>#{group[cname].toLocaleString()}</td></tr>"

      return tbody
    )

    $.get('template/job-tooltip.html', (source) ->
      self.template.tooltip.job = Handlebars.compile(source);
    )
    $.get('template/task-tooltip.html', (source) ->
      self.template.tooltip.task = Handlebars.compile(source);
    )


  load: (job, callback) ->
    console.log "Starting data load " + job.id + new Date()

    url = "/inviso/#{@version}/v0/trace/load/#{job.id}?counters=true"

    if settings.directHistoryPath
      url += "&path=#{encodeURIComponent(job['history.uri'])}"

    $.ajax
      url: url
      type: 'GET'
      dataType: 'json'
      success: (traceData) =>
        jobDetails=
          workflowId: null
          id: @getId(traceData)
          name: @getName(traceData)
          type: 'trace'
          data: @processTrace(traceData)
          version: @version
          info: job
          app: this
          xLabel: 'Task Execution Times'
          yLabel: 'Task Attempts'
          legend:
            'Map': colors.MAP
            'Reduce': colors.REDUCE
            'Killed': colors.KILLED
            'Failed': colors.FAILED


        console.log "Trace build complete for: #{job.id}"

        addChart(jobDetails)
        callback(true)
      error: (xhr, status, e) ->
        console.log e
        callback(false)

  renderSearchResult: (col, r) ->
    $td = $('<td></td>')

    switch col.id
      when 'start'
        t = r[col.id]

        if not t? then $td.text(time_format(r.timestamp))

        if t > 0 then $td.text(time_format(t))
      when 'stop'
        t = r[col.id]
        if t > 0 then $td.text(time_format(t))
      when 'duration'
        stop = r['stop']
        start = r['start']

        if not start? or not stop? then break

        stop = if stop > 0 then stop else Date.now()
        $td.text(hhmmss(stop - r['start']))
      when 'job_status'
        s = statusMap[r[col.id]] || {class:'label label-default', text:r[col.id]}
        $td.append($("<span class=\"#{s?.class}\" >#{s?.text}</span>"))
      when 'workflow_id'
        id = r.genie_id || r.query_id

        if ! id?
          break;

        $td.append($a = $("<a>#{id}</a>"))
        $a.attr
          href: "?workflow=#{id}#profiler"
          target: "_blank"
      when 'genie_id'
        id = r.genie_id || r.query_id

        if not id? then break

        $td.append($a = $("<a>#{id}</a>"))
        $a.attr
          href: "?workflow=#{id}#profiler"
          target: "_blank"
      when 'job_id'
        $td.append($a = $("<a>#{r.job_id}</a>"))
        $a.attr
          href: "?job=#{r.job_id}#profiler"
          target: "_blank"
      when 'links'
        if 'job_tracker' of r || 'yarn_proxy' of r
          $a = $('<a/>',
            href: @getHistoryAddress(r)
            target: "_blank"
          ).append($('<img/>', {src: 'img/hadoop.gif', class: 'hadoop-icon'}))
          $td.append($('<div/>').append($a))
        if 'genie_id' of r
          version = if r.genie_version then r.genie_version else ''
          $a = $('<a/>',
            href: "http://go/genie#{version}-prod/genie/v0/jobs/#{r.genie_id}"
            target: "_blank"
          ).append($('<img/>', {src: 'img/genie.gif', class: 'genie-icon'}))
          $td.append($('<div/>').append($a))
        if 'lipstick_url' of r
          url = r.lipstick_url.split(',').random()
          $a = $('<a/>',
            href: "#{url}/graph.html#graph/#{r.genie_id ? r.lipstick_id}"
            target: "_blank"
          ).append($('<img/>', {src: 'img/lipstick.png', class: 'lipstick-icon'}))
          $td.append($('<div/>').append($a))
      when 'version'
        $td.text(r[col.id] ? 'mr1')
      else
        $td.text(r[col.id])

    return $td

  render: (data, $div) ->
    $.get('template/mapreduce.html', (t) ->
      $div.append($(t))

      $div.find('.panel-title').text(data.id)
      $chart = $div.find('.timeseries')
      $chart.series({'series': data})

      $actions = $div.find('.chart-actions')

      $actions.find('.align-time').click(() -> $chart.series('option', 'flat', false))
      $actions.find('.align-left').click(() -> $chart.series('option', 'flat', true))


      $actions.find('.locality').click(() ->
        @toggle = !@toggle;

        if @toggle
          @colorsave = $chart.series('option', 'color')
          $chart.series('option', 'color', (d) ->
            return localityColors[d.attempt.locality] || 'gray'
          )

          $chart.series('option', 'legend', localityColors)
        else
          $chart.series('option', 'color', @colorsave)
          $chart.series('option', 'legend', null)
      )
      $actions.find('.zoom-in').click(()->
        zoom = $chart.series('option', 'barHeight')
        $chart.series('option', 'barHeight', zoom+1)
      )
      $actions.find('.zoom-out').click(()->
        zoom = $chart.series('option', 'barHeight')
        if zoom <= 1
          return

        $chart.series('option', 'barHeight', zoom-1)
      )
      $actions.find('.toggle-legend').click(()->

        $chart.series('option', 'showLegend', !$chart.series('option', 'showLegend'))
      )
      $actions.find('.close').click(()->$div.remove())
    )

  tooltip: (job, entry) ->
    if ! entry?
      tt = @template.tooltip.job({ job: job })
    else
      tt = @template.tooltip.task( { job: job, entry: entry })

    return tt



class @MapReduce1 extends MapReduceBase
  constructor: ->
    super('mr1')

  getId: (job) -> job.id
  getName: (job) -> job.name
  getHistoryAddress: (r) -> "http://#{r.job_tracker.split(':')[0]}:9100/jobdetails.jsp?jobid=#{r.job_id}"

  processTrace: (trace) ->
    data =
      status: trace.status,
      counters: trace.counters,
      submit_time: trace.submit_time,
      start_time: trace.launch_time,
      stop_time: trace.finish_time,
      entries: []

    for taskId,task of trace.tasks
      for attemptId,attempt of task.attempts
        data.entries.push
          id: attemptId
          label: attemptId
          start: attempt.start_time
          stop: attempt.finish_time ? Date.now()
          color: colors[attempt.task_status] ? colors[attempt.task_type] ? 'gray'
          task: task
          attempt: attempt
          group: 1

    data.entries.sort((l,r) -> l.task.start_time - r.task.start_time)

    return @normalize(data)


class @MapReduce2 extends MapReduceBase
  constructor: ->
    super('mr2')

  getId: (job) -> job.jobid
  getName: (job) -> job.jobName
  getHistoryAddress: (r) -> "http://#{r.yarn_proxy}/proxy/#{r.job_id.replace('job', 'application')}"

  processTrace: (trace) ->
    data =
      username: trace.userName
      status: trace.jobStatus,
      counters: trace.totalCounters,
      submit_time: trace.submitTime,
      start_time: trace.launchTime,
      stop_time: trace.finishTime,
      entries: []

    for attemptId,attempt of trace.tasks.taskid.attempts
      data.entries.push
        id: attemptId
        label: attemptId
        start: attempt.startTime
        stop: attempt.finishTime ? Date.now()
        color: colors[attempt.status] ? colors[attempt.taskType] ? 'gray'
        task: trace.tasks[attempt.taskid]
        attempt: attempt
        group: 1

    data.entries.sort((l,r) -> l.task.startTime - r.task.startTime)

    return @normalize(data)

  logLink: (job, attempt) ->
    root = job.info['yarn.nodemanager.remote-app-log-dir'];
    owner = job.data.username;
    appId = job.id.replace('job', 'application');
    containerId = attempt.attempt.containerId;
    nodeId = attempt.attempt.hostname+':'+attempt.attempt.port;
    fs = job.info['fs.defaultFS'];

    return "/inviso/mr2/v0/log/load/#{owner}/#{appId}/#{containerId}/#{nodeId}?fs=#{fs}&root=#{root}";
