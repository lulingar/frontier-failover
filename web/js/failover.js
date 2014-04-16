//TODO make a reliable sorting of the hosts-table
//TODO add zoom capability to the history chart

var Failover = new function() {

    var self = this;
    // JS weirdness: By default, when not specified otherwise, functions
    //  have the object "this" pointing to the Global scope (a.k.a. "Window")
    //  So to reference to this object, use "self" instead.

    self.time_chart = dc.seriesChart("#time-chart");
    self.group_chart = dc.pieChart("#group-chart");
    self.squid_chart = dc.pieChart("#squid-chart");
    self.hosts_table = dc.dataTable("#hosts-table");
    self.data_file = "failover.csv";
    self.date_format = d3.time.format("%b %d, %Y %I:%M %p");
    self.sites_legend_item_size = 17;
    self.sites_legend_item_gap = 4;
    self.time_chart_width = 1024;
    self.time_chart_height = 450;
    self.groups_base_dim = 150;
    self.groups_legend_width = 200;
    self.groups_radius = self.groups_base_dim/2 - 15;

    self.start = function() {
        var q;

        q = queue().defer(d3.json, "config.json")
                   .defer(d3.csv, self.data_file);
        q.await(self.setup);
    };

    self.setup = function(error, config, dataset) {

        self.config = config;
        self.period = config.history.period;
        self.extent_span = 3.6e6 * config.history.span;

        self.periodObj = minuteBunch(self.period);
        self.periodRange = self.periodObj.range;
        self.addH = function(p, d) { return p + d.Hits; };
        self.remH = function(p, d) { return p - d.Hits; };
        self.ini = function() { return 0; };

        self.ndx = crossfilter( self.process_data(dataset));
        self.all = self.ndx.groupAll().reduce(self.addH, self.remH, self.ini);
        self.site_D = self.ndx.dimension( function(d) { return d.Sites; })

        self.time_D = self.ndx.dimension( function(d) { return d.Timestamp; });
        self.group_D = self.ndx.dimension( function(d) {
                                    return self.config.groups[d.Group].name;
                                });
        self.squid_D = self.ndx.dimension( function(d) {
                                    return d.IsSquid ? "Squid" : "Worker Node";
                                });
        self.hits_D = self.ndx.dimension( function(d) { return d.Hits; });
        self.time_site_D = self.ndx.dimension( function(d) { return [d.Timestamp, d.Sites]; });
        self.host_D = self.ndx.dimension( function(d) { return d.Host; });
        self.group_G = self.group_D.group().reduce(self.addH, self.remH, self.ini);
        self.squid_G = self.squid_D.group().reduce(self.addH, self.remH, self.ini);
        self.time_sites_G = self.time_site_D.group().reduce(self.addH, self.remH, self.ini);
        self.hits_G = self.hits_D.group().reduce(self.addH, self.remH, self.ini);
        self.site_list = self.site_D.group().all().map( function(d) { return d.key; });
        self.site_names_len = flatten_array( self.site_list.map( function(s) {
                            return s.split('\n').map( function(s){ return s.length });
                        }) );
        self.site_longest_name = Math.max.apply(0, self.site_names_len);
        self.num_lines = 1 + self.site_names_len.length;
        self.sites_legend_space_v = self.num_lines * (self.sites_legend_item_size + 
                                                      self.sites_legend_item_gap);
        self.sites_legend_columns = Math.ceil(self.sites_legend_space_v / (0.9*self.time_chart_height));
        self.sites_legend_space_h = (7 * self.site_longest_name) * self.sites_legend_columns + 20;

        self.update_time_extent(self.period, self.extent_span);

        // The time series
        self.sites_color_scale = hsl_set(self.site_list.length, 70, 50);
        self.time_chart.width(self.time_chart_width)
                  .height(self.time_chart_height)
                  .margins({ top: 30, right: 30 + self.sites_legend_space_h,
                             bottom: 60, left: 70 })
                  .chart( function(c) { return dc.barChart(c) } )
                  .ordinalColors(self.sites_color_scale)
                  .dimension(self.time_site_D)
                  .group(self.time_sites_G)
                  .keyAccessor(function(d) { return d.key[0]; })
                  .seriesAccessor(function(d) { return d.key[1]; })
                  .seriesSort(d3.descending)
                  .title(function(d) { return d.key[1] + ": " + d.value + " Hits"; })
                  .xAxisLabel("Time")
                  .yAxisLabel("Hits")
                  .elasticY(true)
                  .x(d3.time.scale().domain(self.extent))
                  .xUnits(self.periodRange)
                  .renderHorizontalGridLines(true)
                  .legend( dc.legend()
                             .x( 1024-self.sites_legend_space_h ).y(10)
                             .itemWidth(150).itemHeight(self.sites_legend_item_size)
                             .gap(4) )
                  .brushOn(false)
                  .renderlet(function(chart) {
                      chart.selectAll(".dc-legend-item")
                           .on("click", function(d) { self.site_filter(d.name); });
                      chart.selectAll(".sub .bar")
                           .on("click", function(d) { self.site_filter(d.layer); });
                   });

        self.time_chart.xAxis().ticks(d3.time.hours, 2);

        // Add listeners to rotate labels and refresh data table
        var rotate_fun = function(d) {
                return "rotate(-90) translate(-25, -12)";
            };
        var axis_tick_rotate = function(chart) {
                chart.selectAll("svg g g.axis.x g.tick text")
                     .attr("transform", rotate_fun);
            }
        self.time_chart.on("postRedraw", axis_tick_rotate);
        self.time_chart.on("postRender", axis_tick_rotate);

        // Site filtering actions
        self.site_filter = function (name) {
                               var short_n = name.split('\n')[0]
                                             + (name.contains('\n') ? ', ...' : '');
                               self.site_D.filterExact(name);
                               self.time_chart.turnOnControls();
                               dc.redrawAll();
                               // This must be run after redrawAll, else it does not render
                               self.time_chart.select('.filter')
                                              .text(short_n)
                                              .property('title', name);
        }

        // Set color distribution for experience consistency across days
        self.group_colors = {}
        for (var group in self.config.groups) {
            var name = self.config.groups[group].name,
                value = self.config.groups[group].order;
            self.group_colors[name] = value;
        }

        // The group chart
        self.group_chart.width(self.groups_base_dim)
                .height(self.groups_base_dim)
                .radius(self.groups_radius)
                .innerRadius(0.3*self.groups_radius)
                .dimension(self.group_D)
                .group(self.group_G)
                .minAngleForLabel(0)
                .colors(d3.scale.category10())
                .colorAccessor(function(d){ return self.group_colors[d.key]; })
                .title(function(d) { return d.key + ": " + d.value + " Hits"; })
                .label(function (d) {
                    if (self.group_chart.hasFilter() && !self.group_chart.hasFilter(d.key))
                            return "0%";
                        return (100 * d.value / self.all.value()).toFixed(2) + "%";
                    })
                .legend( dc.legend().x(self.groups_base_dim).y(50).gap(10) );

        // The host (squid/not squid) chart
        self.squid_chart.width(self.groups_base_dim)
                .height(self.groups_base_dim)
                .radius(self.groups_radius)
                .innerRadius(0.3*self.groups_radius)
                .dimension(self.squid_D)
                .group(self.squid_G)
                .ordinalColors([hsl_set(1, 100, 40, 100), hsl_set(1, 100, 40, 10)]) 
                .title(function(d) { return d.key + ": " + d.value + " Hits"; })
                .label(function (d) {
                    if (self.squid_chart.hasFilter() && !self.squid_chart.hasFilter(d.key))
                            return "0%";
                        return (100 * d.value / self.all.value()).toFixed(2) + "%";
                    })
                .legend( dc.legend().x(self.groups_base_dim).y(50).gap(10) )

        // Table widget for displaying failover details
        self.sort_order = {false: d3.ascending,
                           true: d3.descending};
        self.current_sort_order = false;
        self.table_field_map = { 'Host': 'Host', 'Is Squid?': 'IsSquid',
                                 'Time': 'Timestamp', 'Hits': 'Hits',
                                 'Bandwidth' : 'Bandwidth' };
        self.hosts_table_filter_control = d3.select('#ht-reset'); 
        self.hosts_table.dimension(self.site_D)
                .group(function(d) { return d.Sites.replace(/\n/g, ' | '); })
                .columns([
                        function(d) {
                            var host = d.Host,
                                alias = ( d.Alias === '' ? host : d.Alias );
                            return '<span title="IP: ' + d.Ip + '">' + alias + '</span>';
                        },
                        function(d) {
                            var spec = d.IsSquid ? "Yes" : "No";
                            return '<div class="squid-box ' + spec + '">' + spec + '</div>';
                        },
                        function(d) { return self.date_format(d.Timestamp); },
                        function(d) { return '<span title="~ ' + d.HitsRate.toFixed(2) + ' queries/sec">' + d.Hits + '</span>'; },
                        function(d) { return '<span title="~ ' + size_natural(d.BandwidthRate) + '/sec">' + size_natural(d.Bandwidth) + '</span>'; }
                        ])
                .sortBy(dc.pluck('Timestamp'))
                .order(d3.descending)
                .size(Infinity)
                .renderlet(function(table){
                        table.selectAll(".dc-table-group").classed("info", true);
                 })
                .renderlet(function(table){
                        table.selectAll(".dc-table-column._0")
                             .on('click', function(){
                                 var host = d3.select(this).select('span').text();
                                 self.host_D.filterExact(host);
                                 self.hosts_table_filter_control.selectAll('.reset')
                                                                .style('display', null);
                                 self.hosts_table_filter_control.selectAll('.filter')
                                                                .style('display', null);
                                 dc.redrawAll();
                                 // This must be run after redrawAll, else it does not render
                                 self.hosts_table_filter_control.selectAll('.filter')
                                                                .text('Host ' + host);
                              })
                 });

        // Sorting functionality of fields
        self.hosts_table_headers = self.hosts_table.selectAll('thead th');
        self.hosts_table_headers.on("click", function(d){
            var header = d3.select(this),
                selected = header.select('.header-text').text(),
                field = self.table_field_map[selected],
                glyph = d3.select(this).select('.glyphicon'),
                all = self.hosts_table_headers.select('.glyphicon');

            self.hosts_table_headers.classed('header-inactive', function() {
                    var current = d3.select(this).select('.header-text').text();
                    return !(current == selected);
                });
            self.hosts_table_headers.classed('header-active', function() {
                    var current = d3.select(this).select('.header-text').text();
                    return (current == selected);
                });

            self.current_sort_order = !self.current_sort_order;
            glyph.classed({'glyphicon-chevron-down': self.current_sort_order,
                           'glyphicon-chevron-up': !self.current_sort_order});

            self.hosts_table.order(self.sort_order[self.current_sort_order]);
            self.hosts_table.sortBy(dc.pluck(field));
            dc.redrawAll();
        });

        // Draw all objects
        dc.renderAll();
    };

    self.process_data = function(dataset) {
        var dataset = dataset;

        dataset.forEach( function(d) {
            // The timestamp points to the end of a period.
            //  this must be accounted for for plotting.
            d.Timestamp = new Date((+d.Timestamp - 3600) * 1000);
            d.Timestamp.setMinutes(0);
            d.Timestamp.setSeconds(0);

            d.Hits = +d.Hits;
            d.HitsRate = +d.HitsRate;
            d.Bandwidth = +d.Bandwidth;
            d.BandwidthRate = +d.BandwidthRate;
            d.IsSquid = (d.IsSquid == "True");
            d.Sites = d.Sites.replace(/; /g, '\n');
        });

        return dataset;
    };

    self.reload = function() {
        d3.csv( self.data_file,
                function (error, dataset) {
                    self.ndx.remove();
                    self.ndx.add( self.process_data(dataset));
                    self.update_time_extent(self.period, self.extent_span);
                    dc.redrawAll();
                } );
    };

    self.update_time_extent = function(period, extent_span) {

        var periodObj = minuteBunch(period),
            periodRange = periodObj.range,
            hour = 3.6e6,
            now = new Date(),
            this_hour = periodObj(now).getTime(),
            extent = [new Date(this_hour - extent_span),
                      new Date(this_hour)],
            extent_pad = [new Date(this_hour - extent_span - hour),
                          new Date(this_hour + hour)];

        // Show the currently plotted time span
        d3.select("#date-start")
          .attr("datetime", extent[0])
          .text(self.date_format(extent[0]));
        d3.select("#date-end")
          .attr("datetime", extent[1])
          .text(self.date_format(extent[1]));

        self.extent = extent_pad;
        self.time_chart.x(d3.time.scale().domain(self.extent));
    };

    self.time_chart_reset = function() {
        self.site_D.filterAll();
        self.time_chart.turnOffControls();
        dc.redrawAll();
    };

    self.hosts_table_reset = function() {
        self.host_D.filterAll();
        self.hosts_table_filter_control.selectAll('.reset')
                                       .style('display', 'none');
        self.hosts_table_filter_control.selectAll('.filter')
                                       .style('display', 'none');
        dc.redrawAll();
    };
}

Failover.start();
