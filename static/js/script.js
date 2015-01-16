$(document).ready(function(){

	$("#source").change(function(){
    	var val = $(this).val();
    	if (val == "hashtags"){
    		$("#hashtags_holder").fadeIn();
    	} else {
    		$("#hashtags_holder").hide();
    	}
    	queryData();
    });
	
	$("#pivot").change(function(){
		var val = $(this).val();
    	if (val == "location"){
    		$('#charttype option[value=map]').attr('selected','selected');
    		$('#charttype').prop('disabled', true);
		} else if (val == "hour"){
    		$('#charttype option[value=timeseries]').attr('selected','selected');
    		$('#charttype').prop('disabled', true);
		} else {
    		$('#charttype').prop('disabled', false);
		}
    	queryData();
    });
	
	$("#charttype").change(function(){
    	queryData();
    });
    
    $("#source").change();
	
})

function queryData() {

	var source = $("#source").val();
	var pivot = $("#pivot").val();

	// chart type: http://c3js.org/examples.html
	var chart = $("#charttype").val();

	var args = null;
	
//	$("#map").hide();
//	$("#chart").hide();
	
	if (chart == 'donut') {

		args = {
			bindto : '#chart',
			data : {
				columns : [ [ 'data1', 30 ], [ 'data2', 120 ], ],
				type : 'donut',
				onclick : function(d, i) {
					console.log("onclick", d, i);
				},
				onmouseover : function(d, i) {
					console.log("onmouseover", d, i);
				},
				onmouseout : function(d, i) {
					console.log("onmouseout", d, i);
				}
			},
			donut : {
				title : "Iris Petal Width"
			}
		}

	} else if (chart == 'line') {
		
		args = {
			bindto : '#chart',
		    data: {
		        columns: [
		            ['data1', 30, 200, 100, 400, 150, 250],
		            ['data2', 50, 20, 10, 40, 15, 25]
		        ]
		    }
		}
		
	} else if (chart == 'timeseries') {

		args = {
			bindto : '#chart',
			data : {
				x : 'x',
				// xFormat: '%Y%m%d', // 'xFormat' can be used as custom format
				// of 'x'
				columns : [
						[ 'x', '2013-01-01', '2013-01-02', '2013-01-03',
								'2013-01-04', '2013-01-05', '2013-01-06' ],
						// ['x', '20130101', '20130102', '20130103', '20130104',
						// '20130105', '20130106'],
						[ 'data1', 30, 200, 100, 400, 150, 250 ],
						[ 'data2', 130, 340, 200, 500, 250, 350 ] ]
			},
			axis : {
				x : {
					type : 'timeseries',
					tick : {
						format : '%Y-%m-%d'
					}
				}
			}
		}

	} else if (chart == 'map') {
	    var map = new Datamap({element: document.getElementById('map')});
	}
	
	if (chart == 'map'){
		$("#map").fadeIn();
	} else {
		$("#chart").fadeIn();
		makeChart(args);
	}


	/*
	 * $.ajax({ type: "GET", url: "/data", data: { inputData: "" }, dataType:
	 * "json", success: function(response) { console.log(response);
	 * makeChart(response); }, error: function(xhr, errorType, exception) {
	 * console.log('Error occured'); } });
	 */

}

function makeChart(args){
	
	c3.generate(args);

}