$("input#search").click(Query);

function Query() {
	var keywords = $("input#query").val();
	var startDate = $("input#startDate").val();
	console.log(keywords);
	console.log(startDate);
	$.getJSON("http://localhost:8080/query/"+keywords+"/"+startDate, function(data, testStatus) {
		console.log(data);
	});
}


function Initialize() {
	$("#timeline").slider();
	$("#startDate").datepicker({dateFormat: "dd-mm-yy"});
	var myOptions = {
		center: new google.maps.LatLng(-34.397, 150.644),
		zoom: 1,
		mapTypeId: google.maps.MapTypeId.HYBRID
	};
	var map = new google.maps.Map($("#map")[0],
		myOptions);
}

$(document).ready(Initialize);