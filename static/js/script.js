var Page = {

	init : function(){
		$("#error_message_holder").hide();
		$(document.body).on("click", "#alert_dismiss", function(){
			$("#error_message_holder").hide();
		});
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
				error : Page.handle_error 
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
		 $('#ruleModal').modal('hide');
		 $("#error_message").html(request.responseText + " (" + request.status + ": " + error + ")");
		 $("#error_message_holder").show();
	 }

}

var ChartPage = {
		
	init : function(load_tables, autoload) {

		Page.init();
		
		// prevent default browser behaviour
		event.preventDefault();

		if (load_tables){
			var callback = function(){
				$("#query_submit").prop("disabled", false);
			}; 
			if (autoload){
				callback = function(){
					$("#query_submit").prop("disabled", false);
					ChartPage.handleChange();
				}
			}
			
			$("#query_submit").prop("disabled", true);
			TablePage.load_select('#select_table', null, callback);
		}
		
		$('#form').submit(function(event){
			ChartPage.handleChange();
			return false;
		});

	},

	handleChange : function() {
	
		var table = $("#select_table").val();
		var field = $("#field").val();
		var charttype = $("#charttype").val();
		var interval = $("#interval").val();
	
		var args = null;

		if (!table){
			$("#chart").html("<span class='alert alert-warning'>Please select a table.</span>").show();
			return false;
		}

		if (!field){
			$("#chart").html("<span class='alert alert-warning'>Please select a field.</span>").show();
			return false;
		}
	
		$("#chart").hide();
		
		if (charttype == 'donut' || charttype == 'bar' || charttype == 'timeseries') {
	
			var args = {
				table : table,
				field : field,
				charttype : charttype,
				interval : interval
			};
			ChartPage.queryData(table, charttype, args);
	
		}
		
	},
	
	queryData : function(table, charttype, args){
	
		$("#chart").html("<center><img class='loading' src='/static/img/loading.gif'></center>");
		$("#chart").show();
		
		 $.ajax({
			type : "GET",
			url : "/api/table/"+table+"/data",
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

	init_list : function(table_id){
	
		Page.init();

		$(document.body).on("click", ".rule_delete", function(){
			if (confirm('Are you sure?')){
				rule = $(this).data("rule");
				RulePage.delete(rule, function(response){
					RulePage.list(table_id);
				});
			}
		});

		$(document.body).on("click", ".rule_backfill", function(){
			if (confirm('This will populate data for the past 7 days. Are you sure?')){
				rule = $(this).data("rule");
				table = $(this).data("table");
				RulePage.backfill(rule, table);
			}
		});

		RulePage.init_add_dialog(table_id);
		RulePage.list(table_id);
	},
	
	list : function(table_id, callback){
		var url = "/api/rule/list"
		if (table_id){
			url = url + "?table=" + table_id
		}
		Page.list(url, callback)
	},

	// initialization for rule_add_partial.html
	init_add_dialog : function(table_id){
		
		$(document.body).on("click", ".rule_test", function(){
			var rule = $("#rule_text").val();
			var tag = $("#rule_tag").val();
			RulePage.test(rule, RulePage.test_callback)
		});
		
		// any change to rule results in need for re-test
		$("#rule_add").prop("disabled", true);
		$(document.body).on("keypress", "#rule_text", function(){
			$("#rule_add").prop("disabled", true);
		});
	
		$(document.body).on("click", ".rule_add", function(){
			var rule = $("#rule_text").val();
			var tag = $("#rule_tag").val();
			
			RulePage.add(rule, tag, function(response){
				$('#ruleModal').modal('hide');
				RulePage.list(table_id);
			});
			
		});
		
		RulePage.init_import_count("#rule_text");
		TablePage.load_select('#rule_tag', table_id, function(){
		});
	},
	
	init_import_count : function(rule_text_field){
		
		$("#rule_import_count").hide();
	    $('#rule_import').change(function() {
	    	
	    	var rule = $(rule_text_field).val();
	    	
	    	if (!rule){
	    		alert("Please enter a rule to calculate volume of tweets.");
	    		$(this).attr("checked", false);
	    		return false;
	    	}
	    	
	        if($(this).is(":checked")) {
		    	RulePage.test(rule, RulePage.test_callback);
	        } else {
	        	$("#rule_import_count").hide();
	        }
	    });
	    
	},

	add : function(rule, tag, callback){
		 var params = {
			'rule': rule,
			'tag': tag
		 }
		 Page.add("/api/rule/add", params, callback);
	},

	test : function(rule, success, error){
		
    	$("#rule_import_count").fadeIn();
    	$("#rule_import_loading").show();
    	$("#rule_import_text").html("Calculating volume of tweets over last 7 days...")
    	
    	if (!error){
    		error = function (request, status, error){
				$("#rule_import_count").hide();
				Page.handle_error(request, status, error); 
			}
    	}
    	
		 var params = {
			'rule': rule
		 }
		 $.ajax({
			type : "GET",
			url : "/api/rule/test",
			data : params,
			dataType : "json",
			success : success,
			error : error
		});	
	},
	
	test_callback : function(response){
		var count = response['count'];
		$("#rule_import_loading").hide();
		$("#rule_import_text").html(count + " records found.")
		$("#rule_add").prop("disabled", false);
	},

	backfill : function(rule, table, callback){
		 var params = {
			'rule': rule,
			'table': table
		 }
		 $.ajax({
			type : "GET",
			url : "/api/rule/backfill",
			data : params,
			dataType : "json",
			success : callback,
			error : Page.handle_error
		});
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
				$('#ruleModal').modal('hide');
				$("#datasets").html("");
				TablePage.list();
			});
		});
		
		// any change to rule results in need for re-test
		$("#table_add").prop("disabled", true);
		$(document.body).on("keypress", "#table_rules", function(){
			$("#table_add").prop("disabled", true);
		});
		
		$(document.body).on("click", "#rule_test", function(){
			var rule = $("#table_rules").val();
			RulePage.test(rule, TablePage.test_callback)
		});
		
		$(document.body).on("change", "#table_type", function(){
			var dataset = "gnip."
			if ($(this).val() == "twitter"){
				dataset = "twitter."
			}
			$("#table_dataset").val(dataset);
			
		});
		
		RulePage.init_import_count("#table_rules");
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

		// doctor the chart options to fit in table detail
		$("#chart_bird").remove();
		$(".chart_option").first().remove();
		$(".chart_option").each(function(index){
			$(this).removeClass("col-md-3");
			$(this).addClass("col-md-4");
		});
		
		RulePage.init_list(id);
		ChartPage.init();
		
		// auto load first chart
		ChartPage.handleChange();
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
		 Page.delete("/api/table/"+id+"/delete", {}, callback)
	},
	
	test_callback : function(response){
		var count = response['count'];
		$("#rule_import_loading").hide();
		$("#rule_import_text").html(count + " records found.")
		$("#table_add").prop("disabled", false);
	},

	load_select : function(select_id, default_value, callback) {
		
		TablePage.list(function(response){

			$(select_id).html("");
			
			for (var i = 0; i < response.length; i++){
				
				var id = response[i]['id'];
				var dataset = response[i]['datasetId'];
				var table = response[i]['tableId'];
				var pair = dataset + "." + table;
				var value = pair;
				var label = pair;
				option = new Option(label, value);
				if (id == default_value){
					option.selected = true;
				}
				$(select_id).append(option);
				
			}
			
			if (callback){
				callback();
			}
		});
		
	}
	
}

var AdminPage = {
		
	init : function(){
		Page.init();
	}
		
}