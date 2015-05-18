var Page = {
		
	list : function(list_url, callback){

		// if no callback, do default list detail
		if (!callback){
			
			var cols = $("#table_data").parent().find("tr:first th").length;
			
			$("#table_data").html("");
			$("#table_data").append("<tr><td colspan="+cols+"><center><img src='/static/img/loading.gif'></td></tr>");
			
			callback = Page.list_table_default
			
		}
		
		 $.ajax({
				type : "GET",
				url : list_url,
				dataType : "json",
				success : callback,
				error : Page.handle_error, 
			});	
		
	 },
	 
	 handle_error :	function(xhr, errorType, exception) {
		console.log('Error occured');
	 },

	 
	 list_table_default : function(response) {
				
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
	 
	 add : function(add_url, params, callback) {
		 $.ajax({
				type : "GET",
				url : add_url,
				data : params,
				dataType : "json",
				success : callback,
				error : Page.handle_error 
			});	
	 },
	 
	 delete : function(delete_url, params, callback){
		 $.ajax({
				type : "GET",
				url : delete_url,
				data : params,
				dataType : "json",
				success : callback,
				error : Page.handle_error 
			});	
	 }
	 
	 

}

var ChartPage = {
		
	init : function() {

		// prevent default browser behaviour
		event.preventDefault();

		DatasetPage.load_select('#select_table', "#query_submit");
		
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
			error : Page.handle_error 
		});
	
	},
	
	showChart : function (charttype, args){
		
		args['bindto'] = '#chart';
		c3.generate(args);
	
	}

}

var RulePage = {

	init : function(){
	
		DatasetPage.load_select('#rule_tag', '#rule_add');

		$(document.body).on("click", ".rule_delete", function(){
			if (confirm('Are you sure?')){
				ruleid = $(this).data("ruleid");
				RulePage.delete(ruleid, function(response){
					RulePage.list();
				});
			}
		});
	
		$(document.body).on("click", ".rule_add", function(){
			var rule = $("#rule_text").val();
			var tag = $("#rule_tag").val();
			RulePage.add(rule, tag, function(response){
				$('#myModal').modal('hide');
				RulePage.list();
			});
		});

		DatasetPage.import_count("#rule_text");
		
		RulePage.list();
	},
	
	list : function(callback){
		Page.list("/api/rule/list", callback)
	},
	
	add : function(rule, tag, callback){
		 var params = {
			'rule': rule,
			'tag': tag
		 }
		 Page.add("/api/rule/add", params, callback);
	},
	
	delete : function(index, callback){
		 var params = {
			'index': index
		 }
		 Page.delete("/api/rule/delete", params, callback)
	}

}

var DatasetPage = {

	init : function(){
	
		$(document.body).on("click", ".dataset_delete", function(){
			if (confirm('Are you sure?')){
				datasetid = $(this).data("datasetid");
				DatasetPage.delete(datasetid, function(response){
					DatasetPage.list();
				});
			}
		});
	
		$(document.body).on("click", ".dataset_add", function(){
			var name = $("#dataset_name").val();
			var type = $("#dataset_type").val();
			var rules = $("#dataset_rules").val();
			var imprt = $("#dataset_imprt").val();
			DatasetPage.add(name, type, rules, imprt, function(response){
				$('#myModal').modal('hide');
				$("#datasets").html("");
				DatasetPage.list();
			});
		});
		
		DatasetPage.import_count("#dataset_rules");
	    
		DatasetPage.list();
	},
	
	list : function(callback){
		Page.list("/api/dataset/list", callback)
	},
	
	add : function(name, type, rules, imprt, callback){
		 var params = {
			'name': name,
			'type': type,
			'rules': rules,
			'import': imprt
		 }
		Page.add("/api/dataset/add", params, callback)
	},
	
	delete : function(index, callback){
		 var params = {
			'index': index
		 }
		 Page.delete("/api/dataset/delete", params, callback)
	},
	
	load_select : function(select_id, disable_id) {
		
		$(disable_id).prop("disabled", true);
		
		DatasetPage.list(function(response){

			$(select_id).html("");
			
			for (var i = 0; i < response.length; i++){
				
				var dataset = response[i]['datasetId'];
				var table = response[i]['tableId'];
				var pair = dataset + "." + table;
				var value = pair;
				var label = pair;
				$(select_id).append(new Option(label, value));
				
			}
			
			$(disable_id).prop("disabled", false);
		});
		
	},
	
	import_count : function(rule_input_field){
		
		$("#dataset_import_count").hide();
	    $('#dataset_import').change(function() {
	    	
	    	var rule = $(rule_input_field).val();
	    	
	    	if (!rule){
	    		alert("Please enter a rule to calculate volume of tweets.");
	    		$(this).attr("checked", false);
	    		return false;
	    	}
	    	
	        if($(this).is(":checked")) {
	        	$("#dataset_import_count").fadeIn();
	        } else {
	        	$("#dataset_import_count").hide();
	        }
	    });
	    
	}

	
}

var AdminPage = {
		
	init : function(){
		
	}
		
}