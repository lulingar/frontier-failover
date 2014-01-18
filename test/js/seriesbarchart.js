seriesBarChart = function (parent, chartGroup) {
    var _chart = dc.compositeChart(parent, chartGroup);

    var _charts = {};
    var _chartFunction = dc.barChart;
    var _seriesAccessor;
    var _seriesSort = d3.ascending;
    var _valueSort = keySort;

    _chart._mandatoryAttributes().push('seriesAccessor','chart');
    _chart.shareColors(true);

    function keySort(a,b) {
        return d3.ascending(_chart.keyAccessor()(a), _chart.keyAccessor()(b));
    }

    _chart._preprocessData = function () {
        var keep = [];
        var children_changed;
        var nester = d3.nest().key(_seriesAccessor);
        if(_seriesSort)
            nester.sortKeys(_seriesSort);
        if(_valueSort)
            nester.sortValues(_valueSort);
        var nesting = nester.entries(_chart.data());
        var children =
            nesting.map( function(sub, i) {
                var subChart = _charts[sub.key] || _chartFunction.call(_chart, _chart, chartGroup, sub.key, i);
                if(!_charts[sub.key])
                    children_changed = true;
                _charts[sub.key] = subChart;
                keep.push(sub.key);
                return subChart
                    .dimension(_chart.dimension())
                    .group({all:d3.functor(sub.values)}, sub.key)
                    .keyAccessor(_chart.keyAccessor())
                    .valueAccessor(_chart.valueAccessor())
                    .barPadding(0.1);
            });
        // this works around the fact compositeChart doesn't really
        // have a removal interface
        Object.keys(_charts)
            .filter(function(c) {return keep.indexOf(c) === -1;})
            .forEach(function(c) {
                clearChart(c);
                children_changed = true;
            });
        _chart._compose(children);
        if(children_changed && _chart.legend())
            _chart.legend().render();
    };

    function clearChart(c) {
        if(_charts[c].g())
            _charts[c].g().remove();
        delete _charts[c];
    }

    function resetChildren() {
        Object.keys(_charts).map(clearChart);
        _charts = {};
    }

    _chart.chart = function(_) {
        if (!arguments.length) return _chartFunction;
        _chartFunction = _;
        resetChildren();
        return _chart;
    };

    /**
#### .seriesAccessor([accessor])
Get or set accessor function for the displayed series. Given a datum, this function
should return the series that datum belongs to.
**/
    _chart.seriesAccessor = function(_) {
        if (!arguments.length) return _seriesAccessor;
        _seriesAccessor = _;
        resetChildren();
        return _chart;
    };

    /**
#### .seriesSort([sortFunction])
Get or set a function to sort the list of series by, given series values.

Example:
```
chart.seriesSort(d3.descending);
```
**/
    _chart.seriesSort = function(_) {
        if (!arguments.length) return _seriesSort;
        _seriesSort = _;
        resetChildren();
        return _chart;
    };

    /**
#### .valueSort([sortFunction])
Get or set a function to the sort each series values by. By default this is
the key accessor which, for example, a will ensure lineChart a series connects
its points in increasing key/x order, rather than haphazardly.
**/
    _chart.valueSort = function(_) {
        if (!arguments.length) return _valueSort;
        _valueSort = _;
        resetChildren();
        return _chart;
    };

    // make compose private
    _chart._compose = _chart.compose;
    delete _chart.compose;

    return _chart;
};

