var ChartPage = {
		
	init : function() {

		// prevent default browser behaviour
		event.preventDefault();

		$('#form').submit(function(event){
			ChartPage.handleChange();
			return false;
		});
		
	},

	handleChange : function() {
	
		var source = $("#source").val();
		var charttype = $("#charttype").val();
		var interval = $("#interval").val();
	
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
				interval : interval
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

var RulesPage = {

	init : function(){
	
		$(document.body).on("click", ".rule_delete", function(){
			if (confirm('Are you sure?')){
				ruleid = $(this).data("ruleid");
				RulesPage.delete(ruleid, function(response){
					RulesPage.list();
				});
			}
		});
	
		$(document.body).on("click", ".rule_add", function(){
			var rule = $("#rule_text").val();
			var tag = $("#rule_tag").val();
			RulesPage.add(rule, tag, function(response){
				$('#myModal').modal('hide');
				RulesPage.list();
			});
		});
	
		RulesPage.list();
	},
	
	list : function(callback){
		$("#rules").html("");
		 $.ajax({
				type : "GET",
				url : "/rules/list",
				dataType : "json",
				success : function(response) {
					
					template = $("#ruleRow").html();
					Mustache.parse(template);
					
					for (var i = 0; i < response.length; i++){
						
						var rule = response[i];
						rule['count'] = i;
						
						var output = Mustache.render(template, rule);
						$("#rules").append(output);
						
					}				
				},
				error : function(xhr, errorType, exception) {
					console.log('Error occured');
				}
			});	
	},
	
	add : function(rule, tag, callback){
		 var params = {
			'rule': rule,
			'tag': tag
		 }
		 $.ajax({
				type : "GET",
				url : "/rules/add",
				data : params,
				dataType : "json",
				success : callback,
				error : function(xhr, errorType, exception) {
					console.log('Error occured');
				}
			});	
	},
	
	delete : function(index, callback){
		 var params = {
			'index': index
		 }
		 $.ajax({
				type : "GET",
				url : "/rules/delete",
				data : params,
				dataType : "json",
				success : callback,
				error : function(xhr, errorType, exception) {
					console.log('Error occured');
				}
			});	
	}

}

var DatasetsPage = {

	init : function(){
	
		$(document.body).on("click", ".dataset_delete", function(){
			if (confirm('Are you sure?')){
				datasetid = $(this).data("datasetid");
				delete(datasetid, function(response){
					DatasetsPage.list();
				});
			}
		});
	
		$(document.body).on("click", ".dataset_add", function(){
			var dataset = $("#dataset_text").val();
			var tag = $("#dataset_tag").val();
			DatasetsPage.add(dataset, tag, function(response){
				$('#myModal').modal('hide');
				$("#datasets").html("");
				DatasetsPage.list();
			});
		});
	
		DatasetsPage.list();
	},
	
	list : function(callback){
		 $.ajax({
				type : "GET",
				url : "/datasets/list",
				dataType : "json",
				success : function(response) {
					
					template = $("#datasetRow").html();
					Mustache.parse(template);
					
					for (var i = 0; i < response.length; i++){
						
						var dataset = response[i];
						dataset['count'] = i;
						
						var output = Mustache.render(template, dataset);
						
						$("#datasets").append(output);
						
					}				
				},
				error : function(xhr, errorType, exception) {
					console.log('Error occured');
				}
			});	
	},
	
	add : function(dataset, tag, callback){
		 var params = {
			'dataset': dataset,
			'tag': tag
		 }
		 $.ajax({
				type : "GET",
				url : "/datasets/add",
				data : params,
				dataType : "json",
				success : callback,
				error : function(xhr, errorType, exception) {
					console.log('Error occured');
				}
			});	
	},
	
	delete : function(index, callback){
		 var params = {
			'index': index
		 }
		 $.ajax({
				type : "GET",
				url : "/datasets/delete",
				data : params,
				dataType : "json",
				success : callback,
				error : function(xhr, errorType, exception) {
					console.log('Error occured');
				}
			});	
	}
	
}

var AdminPage = {
		
	init : function(){
		
	}
		
}