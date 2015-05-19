var Page = {

	init : function(){
		$("#error_message_holder").hide();
	},
		
	list : function(list_url, callback){

		// if no callback, do default list detail
		if (!callback){
			
			var cols = $("#table_data").parent().find("tr:first th").length;
			
			$("#table_data").html("");
			$("#table_data").append("<tr><td colspan="+cols+"><center><img class='loading' src='/static/img/loading.gif'></td></tr>");
			
			callback = Page.list_default
			
		}

		 $.ajax({
				type : "GET",
				url : list_url,
				dataType : "json",
				success : callback,
				error : Page.handle_error, 
			});	
		
	 },
	 
	 list_default : function(response) {
				
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
	 
	 detail : function(detail_url, callback) {
		 
		// if no callback, do default list detail
		if (!callback){
			
//			var cols = $("#table_data").parent().find("tr:first th").length;
//			
//			$("#table_data").html("");
//			$("#table_data").append("<tr><td colspan="+cols+"><center><img class='loading' src='/static/img/loading.gif'></td></tr>");
			
			callback = Page.detail_default
			
		}
		
		 $.ajax({
				type : "GET",
				url : detail_url,
				dataType : "json",
				success : callback,
				error : Page.handle_error 
			});	
	 },
	 
	 detail_default : function(response){
		 alert(1);
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
	 },
	 
	 handle_error :	function(request, status, error) {
		 $('#myModal').modal('hide');
		 $("#error_message").html(request.responseText + " (" + request.status + ": " + error + ")");
		 $("#error_message_holder").show();
	 }

}

var ChartPage = {
		
	init : function() {

		Page.init();
		
		// prevent default browser behaviour
		event.preventDefault();

		TablePage.load_select('#select_table', "#query_submit");
		
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
	
		$("#chart").html("<img class='loading' src='/static/img/loading.gif'>");
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

	init_list : function(){
	
		Page.init();

		$(document.body).on("click", ".rule_delete", function(){
			if (confirm('Are you sure?')){
				value = $(this).data("value");
				RulePage.delete(value, function(response){
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

		TablePage.load_select('#rule_tag', '#rule_add');
		TablePage.init_import_count("#rule_text");
		
		RulePage.list();
	},
	
	list : function(table, callback){
		var url = "/api/rule/list"
		if (table){
			url = url + "?table=" + table
		}
		Page.list(url, callback)
	},
	
	add : function(rule, tag, callback){
		 var params = {
			'rule': rule,
			'tag': tag
		 }
		 Page.add("/api/rule/add", params, callback);
	},
	
	delete : function(value, callback){
		 var params = {
			'value': value
		 }
		 Page.delete("/api/rule/delete", params, callback)
	}

}

var TablePage = {

	init_list : function(){

		Page.init();

		$(document.body).on("click", ".table_delete", function(){
			if (confirm('Are you sure?')){
				datasetid = $(this).data("id");
				TablePage.delete(datasetid, function(response){
					TablePage.list();
				});
			}
		});
	
		$(document.body).on("click", ".table_add", function(){
			var dataset = $("#table_dataset").val();
			var table = $("#table_name").val();
			var type = $("#table_type").val();
			var rules = $("#table_rules").val();
			var imprt = $("#table_imprt").val();
			
			TablePage.add(dataset, table, type, rules, imprt, function(response){
				$('#myModal').modal('hide');
				$("#datasets").html("");
				TablePage.list();
			});
		});
		
		$(document.body).on("change", "#table_type", function(){
			var dataset = "gnip."
			if ($(this).val() == "twitter"){
				dataset = "twitter."
			}
			$("#table_name").val(dataset);
			
		});
		
		TablePage.init_import_count("#table_rules");
	    
		TablePage.list();
	},
	
	list : function(callback){
		Page.list("/api/table/list", callback)
	},
	
	init_detail : function(id){
		
		Page.init();

		$(document.body).on("click", ".table_delete", function(){
			if (confirm('Are you sure?')){
				datasetid = $(this).data("id");
				TablePage.delete(datasetid, function(response){
					window.location = "/table/list"
				});
			}
		});

		$(document.body).on("click", ".rule_delete", function(){
			if (confirm('Are you sure?')){
				value = $(this).data("value");
				RulePage.delete(value, function(response){
					RulePage.list(id);
				});
			}
		});

		$(document.body).on("click", ".rule_add", function(){
			var rule = $("#rule_text").val();
			var tag = $("#rule_tag").val();
			RulePage.add(rule, tag, function(response){
				$('#myModal').modal('hide');
				RulePage.list(id);
			});
		});

		TablePage.load_select('#rule_tag', '#rule_add');
		TablePage.init_import_count("#rule_text");

		RulePage.list(id);
	},
	
	detail : function(id, callback){
		Page.detail("/api/table/" + id, callback)
	},
	
	add : function(dataset, table, type, rules, imprt, callback){
		 var params = {
			'dataset': dataset,
			'name': table,
			'type': type,
			'rules': rules,
			'import': imprt
		 }
		Page.add("/api/table/add", params, callback)
	},
	
	delete : function(id, callback){
		 var params = {
			'id': id
		 }
		 Page.delete("/api/table/delete", params, callback)
	},
	
	load_select : function(select_id, disable_id) {
		
		$(disable_id).prop("disabled", true);
		
		TablePage.list(function(response){

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
	
	init_import_count : function(rule_input_field){
		
		$("#table_import_count").hide();
	    $('#table_import').change(function() {
	    	
	    	var rule = $(rule_input_field).val();
	    	
	    	if (!rule){
	    		alert("Please enter a rule to calculate volume of tweets.");
	    		$(this).attr("checked", false);
	    		return false;
	    	}
	    	
	        if($(this).is(":checked")) {
	        	$("#table_import_count").fadeIn();
	        } else {
	        	$("#table_import_count").hide();
	        }
	    });
	    
	}

	
}

var AdminPage = {
		
	init : function(){
		
	}
		
}