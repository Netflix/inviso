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

class @Inviso
  constructor: (args) ->
    @apps =
        mr1: new MapReduce1()
        mr2: new MapReduce2()

    @search =
      results: ko.observableArray([])
      index: 0
      total: 0
      searching: ko.observable(false)
      columns: searchFields

      reset: =>
        @search.results([])
        @search.index = 0
        @search.total = 0
        $('.search-result').empty()

      canSearch: ->
        !@searching() && @index < @total

    @loading =
      total: ko.observable(0)
      complete: ko.observable(0)

    @workflows = ko.observableArray([])

    @cluster =
      selected: ko.observable()
      start: ko.observable()
      end: ko.observable()
      refresh: ko.observable(60000)
      tooltip: {}

    $.get('template/cluster-tooltip.html', (source) =>
      @cluster.tooltip.app = Handlebars.compile(source);
    )

  closeWorkflow: (workflow)->
