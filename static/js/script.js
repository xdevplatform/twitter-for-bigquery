var Page = {
		
	list : function(list_url){

		var cols = $("#table_data").parent().find("tr:first th").length;
		
		$("#table_data").html("");
		$("#table_data").append("<tr><td colspan="+cols+"><center><img src='/static/img/loading.gif'></td></tr>");
		
		 $.ajax({
				type : "GET",
				url : list_url,
				dataType : "json",
				success : function(response) {
					
					$("#table_data").html("");
					
					template = $("#table_row").html();
					Mustache.parse(template);
					
					for (var i = 0; i < response.length; i++){
						
						var rule = response[i];
						rule['count'] = i;
						
						var output = Mustache.render(template, rule);
						$("#table_data").append(output);
						
					}				
				},
				error : function(xhr, errorType, exception) {
					console.log('Error occured');
				}
			});	
	 },
	 
	 add : function(add_url, params, callback) {
		 $.ajax({
				type : "GET",
				url : add_url,
				data : params,
				dataType : "json",
				success : callback,
				error : function(xhr, errorType, exception) {
					console.log('Error occured');
				}
			});	
	 },
	 
	 delete : function(delete_url, params, callback){
		 $.ajax({
				type : "GET",
				url : delete_url,
				data : params,
				dataType : "json",
				success : callback,
				error : function(xhr, errorType, exception) {
					console.log('Error occured');
				}
			});	
	 }
	 
	 

}

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
		Page.list("/rules/list")
	},
	
	add : function(rule, tag, callback){
		 var params = {
			'rule': rule,
			'tag': tag
		 }
		 Page.add("/rules/add", params,callback);
	},
	
	delete : function(index, callback){
		 var params = {
			'index': index
		 }
		 Page.delete("/rules/delete", params, callback)
	}

}

var DatasetsPage = {

	init : function(){
	
		$(document.body).on("click", ".dataset_delete", function(){
			if (confirm('Are you sure?')){
				datasetid = $(this).data("datasetid");
				DatasetsPage.delete(datasetid, function(response){
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
		Page.list("/datasets/list")
	},
	
	add : function(dataset, tag, callback){
		 var params = {
			'dataset': dataset,
			'tag': tag
		 }
			Page.add("/datasets/add", params, callback)
	},
	
	delete : function(index, callback){
		 var params = {
			'index': index
		 }
		 Page.delete("/datasets/delete", params, callback)
	}
	
}

var AdminPage = {
		
	init : function(){
		
	}
		
}