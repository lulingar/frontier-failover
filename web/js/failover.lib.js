minuteBunch = function(numMinutes) {
    var bunch_ms = numMinutes * 6e4;

    return d3.time.interval( 
	function(date) {
	    var timezone = date.getTimezoneOffset();
	    return new Date(Math.floor( (date )/bunch_ms ) * bunch_ms);
	}, 
	function(date, offset) {
	    date.setTime(date.getTime() + Math.floor(offset) * bunch_ms); // DST breaks setMinutes
	}, 
        function(date) {
            var minutes = date.getMinutes();
            var number = Math.floor(minutes / numMinutes) * numMinutes;
	    return number;
	}
    );
}

function size_natural (size) 
{
    if (size == 0) return "0 B";

    var scales = {'B': 0, 'kiB': 10, 'MiB': 20, 'GiB': 30, 'TiB': 40};

    var proportion = []
    for (var key in scales) {
        proportion.push([Math.abs(1 - ((Math.pow(2, scales[key])) / size)), key]);
    }
    proportion.sort();
    var scale = proportion[0][1];

    var base_part = size / Math.pow(2, scales[scale]);
    var size_str = base_part.toFixed(2) + ' ' + scale; 

    return size_str;
}

function flatten_array (arr)
{
    var flat = [];
    flat = flat.concat.apply(flat, arr);
    return flat;
}
