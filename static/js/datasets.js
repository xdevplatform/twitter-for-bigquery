$(document).ready(function(){

	init();

});

function init(){

	$(document.body).on("click", ".dataset_delete", function(){
		if (confirm('Are you sure?')){
			datasetid = $(this).data("datasetid");
			datasets_delete(datasetid, function(response){
				datasets_list();
			});
		}
	});

	$(document.body).on("click", ".dataset_add", function(){
		var dataset = $("#dataset_text").val();
		var tag = $("#dataset_tag").val();
		datasets_add(dataset, tag, function(response){
			$('#myModal').modal('hide');
			$("#datasets").html("");
			datasets_list();
		});
	});

	datasets_list();
}

function datasets_list(callback){
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
}

function datasets_add(dataset, tag, callback){
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
}

function datasets_delete(index, callback){
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