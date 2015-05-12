$(document).ready(function(){

//	init();

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

	// rules_list();
}

