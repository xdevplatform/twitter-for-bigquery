$(document).ready(function(){

	init();

});

function init(){

	$(document.body).on("click", ".rule_delete", function(){
		if (confirm('Are you sure?')){
			ruleid = $(this).data("ruleid");
			rules_delete(ruleid, function(response){
				rules_list();
			});
		}
	});

	$(document.body).on("click", ".rule_add", function(){
		var rule = $("#rule_text").val();
		var tag = $("#rule_tag").val();
		rules_add(rule, tag, function(response){
			$('#myModal').modal('hide');
			$("#rules").html("");
			rules_list();
		});
	});

	rules_list();
}

function rules_list(callback){
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
}

function rules_add(rule, tag, callback){
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
}

function rules_delete(index, callback){
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