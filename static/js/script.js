var ChartPage = {
		
	init : function() {

		// prevent default browser behaviour
		event.preventDefault();

		ChartPage.handleChange();
		
	},

	getMentions : function(){
		return ChartPage.getValuesByClass('.mention');
	},
	
	getHashtags : function(){
		return ChartPage.getValuesByClass('.hashtag');
	},
	
	getValuesByClass : function(sel){
		var arr = [];
		$(sel).each( function(i,e) {
			val = $(e).val();
			if (val){
				arr.push(val);
			}
		});
		return arr.join();
	},
	
	handleChange : function() {
	
		var source = $("#source").val();
		var charttype = $("#charttype").val();
		var interval = $("#interval").val();
		var hashtags = ChartPage.getHashtags();
		var mentions = ChartPage.getMentions();
	
		var args = null;
	
		if (!source){
			$("#chart").html("<span class='alert alert-warning'>Please select a source.</span>").show();
			return false;
		}
	
		$("#map").hide();
		$("#chart").hide();
		
		if (charttype == 'donut' || charttype == 'bar' || charttype == 'timeseries') {
	
			var args = {
				source : source,
				charttype : charttype,
				interval : interval,
				terms : source == 'hashtags' ? hashtags : source == 'mentions' ? mentions : null  
			};
			ChartPage.queryData(charttype, args);
	
		}
		
	},
	
	queryData : function(charttype, args){
	
		$("#chart").html('<img src="/static/img/loading.gif">');
		$("#chart").show();
		
		 $.ajax({
			type : "GET",
			url : "/chart/data",
			data : args,
			dataType : "json",
			success : function(response) {
				
				console.log(response);
				
				if (response && response.constructor == Object){
					ChartPage.showChart(charttype, response);
					$("#chart").fadeIn();
					
					$("#query").val(response['query']);
					$("#title").val(response['title']);
	
				} else {
					$("#chart").html('<h4>Not yet supported</h4>');
					$("#chart").fadeIn();
				} 
				
			},
			error : function(xhr, errorType, exception) {
				console.log('Error occured');
			}
		});
	
	},
	
	showChart : function (charttype, args){
		
		args['bindto'] = '#chart';
		c3.generate(args);
	
	}

}