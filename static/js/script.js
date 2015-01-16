$(document).ready(function(){

	$("#source").change(function(){
    	var val = $(this).val();
    	if (val == "hashtags"){
    		$("#hashtags_holder").fadeIn();
    	} else {
    		$("#hashtags_holder").hide();
    	}
    	handleChange();
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
    	handleChange();
    });
	
	$("#charttype").change(function(){
		handleChange();
    });
    
    $("#source").change();
	
})

function handleChange() {

	var source = $("#source").val();
	var pivot = $("#pivot").val();

	// chart type: http://c3js.org/examples.html
	var charttype = $("#charttype").val();

	var args = null;
	
	$("#map").hide();
	$("#chart").hide();
	
	if (charttype == 'donut' || charttype == 'bar' || charttype == 'timeseries') {

		var args = {
			source : source,
			pivot : pivot,
			charttype : charttype,
			hashtags : []
		};
		queryData(args);

	} else if (charttype == 'map') {

		$("#map").show().html("");
		showMap();
		
	}
	
}

function queryData(args){

	$("#chart").html('<img src="/static/img/loading.gif">');
	$("#chart").show();
	
	 $.ajax({
		type : "GET",
		url : "/data",
		data : args,
		dataType : "json",
		success : function(response) {
			
			console.log(response);
			showChart(response);
			
		},
		error : function(xhr, errorType, exception) {
			console.log('Error occured');
		}
	});

}

function showChart(args){
	
	args['bindto'] = '#chart';
	
	c3.generate(args);
	$("#chart").fadeIn();
}

function showMap(){
	var election = new Datamap({
		scope : 'usa',
		element : document.getElementById('map'),
		geographyConfig : {
			highlightBorderColor : '#bada55',
			popupTemplate : function(geography, data) {
				return '<div class="hoverinfo">' + geography.properties.name
						+ 'Electoral Votes:' + data.electoralVotes + ' '
			},
			highlightBorderWidth : 3
		},

		fills : {
			'Republican' : '#CC4731',
			'Democrat' : '#306596',
			'Heavy Democrat' : '#667FAF',
			'Light Democrat' : '#A9C0DE',
			'Heavy Republican' : '#CA5E5B',
			'Light Republican' : '#EAA9A8',
			defaultFill : '#EDDC4E'
		},
		data : {
			"AZ" : {
				"fillKey" : "Republican",
				"electoralVotes" : 5
			},
			"CO" : {
				"fillKey" : "Light Democrat",
				"electoralVotes" : 5
			},
			"DE" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"FL" : {
				"fillKey" : "UNDECIDED",
				"electoralVotes" : 29
			},
			"GA" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"HI" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"ID" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"IL" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"IN" : {
				"fillKey" : "Republican",
				"electoralVotes" : 11
			},
			"IA" : {
				"fillKey" : "Light Democrat",
				"electoralVotes" : 11
			},
			"KS" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"KY" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"LA" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"MD" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"ME" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"MA" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"MN" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"MI" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"MS" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"MO" : {
				"fillKey" : "Republican",
				"electoralVotes" : 13
			},
			"MT" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"NC" : {
				"fillKey" : "Light Republican",
				"electoralVotes" : 32
			},
			"NE" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"NV" : {
				"fillKey" : "Heavy Democrat",
				"electoralVotes" : 32
			},
			"NH" : {
				"fillKey" : "Light Democrat",
				"electoralVotes" : 32
			},
			"NJ" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"NY" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"ND" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"NM" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"OH" : {
				"fillKey" : "UNDECIDED",
				"electoralVotes" : 32
			},
			"OK" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"OR" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"PA" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"RI" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"SC" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"SD" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"TN" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"TX" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"UT" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"WI" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"VA" : {
				"fillKey" : "Light Democrat",
				"electoralVotes" : 32
			},
			"VT" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"WA" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"WV" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"WY" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"CA" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"CT" : {
				"fillKey" : "Democrat",
				"electoralVotes" : 32
			},
			"AK" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"AR" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			},
			"AL" : {
				"fillKey" : "Republican",
				"electoralVotes" : 32
			}
		}
	});
	election.labels();
}