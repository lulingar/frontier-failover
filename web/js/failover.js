//TODO make a reliable sorting of the hosts-table
//TODO add zoom capability to the history chart

var Failover = new function() {

    var self = this;
    // JS weirdness: By default, when not specified otherwise, functions
    //  have the object "this" pointing to the Global scope (a.k.a. "Window")
    //  So to reference to this object, use "self" instead.

    self.config_file = "config.json";
    self.time_chart = dc.seriesChart("#time-chart");
    self.group_chart = dc.pieChart("#group-chart");
    self.squid_chart = dc.pieChart("#squid-chart");
    self.hosts_table = dc.dataTable("#hosts-table");
    self.date_format = d3.time.format("%b %d, %Y %I:%M %p");
    self.sites_legend_item_size = 17;
    self.sites_legend_item_gap = 4;
    self.time_chart_width = 1024;
    self.time_chart_height = 450;
    self.groups_base_dim = 150;
    self.groups_legend_width = 200;
    self.groups_radius = self.groups_base_dim/2 - 15;
    self.time_zone_offset = 0;

    self.start = function() {
        d3.json(self.config_file, 
                function (error, config) {
                    self.create_objects(config);
                    queue().defer(d3.csv, self.config["record_file"])
                           .defer(d3.csv, self.config["emails"]["record_file"])
                           .await(self.first_setup);
                });
    };

    self.first_setup = function(error, dataset, emails) {

        self.setup_base();
        self.setup_update(dataset, emails);
        dc.renderAll();
        self.apply_url_filters();
    };

    self.setup_base = function() {

        // The time series chart
        self.time_chart
                  .width(self.time_chart_width)
                  .height(self.time_chart_height)
                  .chart( function(c) { return dc.barChart(c) } )
                  .keyAccessor(function(d) { return d.key[0]; })
                  .seriesAccessor(function(d) { return d.key[1]; })
                  .seriesSort(d3.descending)
                  .title(function(d) { return d.key[1] + ": " + d.value + " Hits"; })
                  .xAxisLabel("Time")
                  .elasticY(true)
                  .renderHorizontalGridLines(true)
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

                               // Blind update of URL query string
                               history.pushState(null, '', '?site=' + encodeURIComponent(name));
        }

        // Set color distribution for color consistency among page visits
        self.group_colors = {}
        for (var group in self.config.groups) {
            var name = self.config.groups[group].name,
                value = self.config.groups[group].order;
            self.group_colors[name] = value;
        }

        // The group chart
        self.group_chart
                .width(self.groups_base_dim)
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
        self.squid_chart
                .width(self.groups_base_dim)
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
                .legend( dc.legend().x(self.groups_base_dim).y(50).gap(10) );

        // Table widget for displaying failover details
        self.sort_order = {false: d3.ascending,
                           true: d3.descending};
        self.current_sort_order = false;
        self.table_field_map = { 'Host': 'Host', 'Is Squid?': 'IsSquid',
                                 'Time': 'Timestamp', 'Hits': 'Hits',
                                 'Bandwidth' : 'Bandwidth' };
        self.hosts_table_filter_control = d3.select('#ht-reset');
        self.hosts_table
                .dimension(self.site_D)
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
                glyph = header.select('.glyphicon'),
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
            self.hosts_table.redraw();
        });
    };

    self.setup_update = function(dataset, emails) {

        self.dataset = dataset;
        self.emails_rec = emails;

        self.set_time_offset();
        self.ndx.add(self.parse_failover_records(dataset));
        self.emails_data = self.parse_email_records(emails);

        self.site_list = self.site_D.group().all().map(dc.pluck('key'));
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

        // The time series chart
        self.sites_color_scale = hsl_set(self.site_list.length, 70, 50);
        self.time_chart
                  .yAxisLabel("Hits")
                  .dimension(self.time_site_D)
                  .group(self.time_sites_G)
                  .margins({ top: 30, right: 30 + self.sites_legend_space_h,
                             bottom: 70, left: 80 })
                  .ordinalColors(self.sites_color_scale)
                  .x(d3.time.scale().domain(self.extent))
                  .xUnits(self.periodRange)
                  .legend( dc.legend()
                             .x( 1024-self.sites_legend_space_h ).y(10)
                             .itemWidth(150).itemHeight(self.sites_legend_item_size)
                             .gap(4) );

        // Email record
        self.email_table_update(self.emails_data);
        d3.select('#email-history').text(self.config["history"]["span"]);
        d3.select('#email-period').text(self.config["emails"]["periodicity"]);
    };

    self.set_time_offset = function() {
        var offsets = {"local": new Date().getTimezoneOffset(),
                       "utc": 0,
                       "cern": -120},
            e = document.getElementById("timezone"),
            chosen_offset = e.options[e.selectedIndex].value;

        // D3 or JS will be default render times in local zone
        self.time_zone_offset = -60e3*(offsets[chosen_offset] - offsets['local']);
    };

    self.email_table_update = function(emails_data) {

        var table = d3.select("#emails-table-div table");
        table.selectAll("tbody").remove();

        var body = table.append("tbody"),
            rows = body.selectAll("tr")
                       .data(emails_data)
                       .enter()
                       .append("tr"),
            columns = [
                        function(d) { return '<ul class="no-bullets"><li>'
                                            + d.Sites.split('\n')
                                               .join(';</li><li>')
                                            + "</li></ul>"; },
                        function(d) { return '<ul class="no-bullets"><li>'
                                            + d.Addresses.join('</li><li>')
                                            + "</li></ul>"; },
                        function(d) { return self.date_format(d.Timestamp); }
                ];

        columns.forEach( function(f, i) {
            rows.append("td")
                .html(f);
        });
    };

    self.create_objects = function(config) {

        self.config = config;

        self.period = config.history.period;
        self.extent_span = 3.6e6 * config.history.span;
        self.periodObj = minuteBunch(self.period);
        self.periodRange = self.periodObj.range;

        self.addH = function(p, d) { return p + d.Hits; };
        self.remH = function(p, d) { return p - d.Hits; };
        self.ini = function() { return 0; };

        self.ndx = crossfilter();
        self.all = self.ndx.groupAll().reduce(self.addH, self.remH, self.ini);

        self.site_D = self.ndx.dimension(dc.pluck('Sites'))
        self.bandwidth_D = self.ndx.dimension(dc.pluck('Bandwidth'));
        self.hits_D = self.ndx.dimension(dc.pluck('Hits'));
        self.host_D = self.ndx.dimension(dc.pluck('Host'));
        self.group_D = self.ndx.dimension( function(d) {
                                    return self.config.groups[d.Group].name;
                                });
        self.squid_D = self.ndx.dimension( function(d) {
                                    return d.IsSquid ? "Squid" : "Worker Node";
                                });
        self.time_site_D = self.ndx.dimension( function(d) { return [d.Timestamp, d.Sites]; });

        self.group_G = self.group_D.group().reduce(self.addH, self.remH, self.ini);
        self.squid_G = self.squid_D.group().reduce(self.addH, self.remH, self.ini);
        self.time_sites_G = self.time_site_D.group().reduce(self.addH, self.remH, self.ini);
        self.hits_G = self.hits_D.group().reduce(self.addH, self.remH, self.ini);
    };

    self.parse_failover_records = function(dataset) {
        // The input element needs to remain intact, so a
        // copy is prepared
        var processed = [];

        dataset.forEach( function(d) {
            var row = {};

            // The timestamp points to the end of a period.
            //  this must be accounted for for plotting.
            row.Timestamp = new Date(1e3*(+d.Timestamp - 3600) + self.time_zone_offset);
            row.Timestamp.setMinutes(0);
            row.Timestamp.setSeconds(0);

            row.Hits = +d.Hits;
            row.HitsRate = +d.HitsRate;
            row.Bandwidth = +d.Bandwidth;
            row.BandwidthRate = +d.BandwidthRate;
            row.IsSquid = (d.IsSquid == "True");
            row.Sites = d.Sites.replace(/; /g, '\n');

            row.Host = d.Host;
            row.Ip = d.Ip;
            row.Alias = d.Alias;
            row.Group = d.Group;

            processed.push(row);
        });

        return processed;
    };

    self.parse_email_records = function(emails) {
        // The input element needs to remain intact, so a
        // copy is prepared
        var processed = [];

        emails.forEach( function(d) {
            var row = {};

            row.Timestamp = new Date(1000 * (+d.Timestamp));
            row.Timestamp.setMinutes(0);
            row.Timestamp.setSeconds(0);

            row.Sites = d.Sites.replace(/; /g, '\n');
            row.Addresses = d.Addresses.replace(/@/g, '_AT_')
                                       .split(", ");

            processed.push(row);
        });

        return processed;
    };

    self.reload = function() {
        queue().defer(d3.csv, self.config["record_file"])
               .defer(d3.csv, self.config["emails"]["record_file"])
               .await(function (error, dataset, emails) {
                    self.ndx.remove();
                    self.setup_update(dataset, emails);
                    dc.redrawAll();
                });
    };

    self.redraw_offset = function() {
        self.ndx.remove();
        self.setup_update(self.dataset, self.emails_rec);
        dc.renderAll();
    };

    self.update_time_extent = function(period, extent_span) {

        var periodObj = minuteBunch(period),
            periodRange = periodObj.range,
            hour = 3.6e6,
            now = new Date(),
            this_hour = periodObj(now).getTime() + self.time_zone_offset,
            extent = [new Date(this_hour - extent_span),
                      new Date(this_hour)],
            extent_pad = [new Date(this_hour - extent_span - hour),
                          new Date(this_hour + hour)];

        // Do show the currently plotted time span
        d3.select("#date-start")
          .attr("datetime", extent[0])
          .text(self.date_format(extent[0]));
        d3.select("#date-end")
          .attr("datetime", extent[1])
          .text(self.date_format(extent[1]));

        self.extent = extent_pad;
    };

    self.time_chart_reset = function() {
        self.site_D.filterAll();
        self.time_chart.turnOffControls();
        dc.redrawAll();
       // Blind update of URL query string
       history.pushState(null, '', '?');
    };

    self.hosts_table_reset = function() {
        self.host_D.filterAll();
        self.hosts_table_filter_control.selectAll('.reset')
                                       .style('display', 'none');
        self.hosts_table_filter_control.selectAll('.filter')
                                       .style('display', 'none');
        dc.redrawAll();
    };

    self.apply_url_filters = function() {
        var url_params = getUrlVars();

        if (url_params.length > 0) {
            if ("site" in url_params) {
                self.site_filter(url_params["site"])
            }
        }
    };
};

Failover.start();
