$("input#search").click(Query);
$(document).ready(Initialize);

// This is going to be the 3D array containing HashtagObjects
var Hashtags = new Array();
var COLORS = ["red", "blue", "yellow", "green", "black"];
var legend_counter = 0;
var Map, StartDate, FilterDate;

function daysBetween ( date1, date2 ) {
	//Get 1 day in milliseconds
	var one_day=1000*60*60*24;

	// Convert both dates to milliseconds
	var date1_ms = date1.getTime();
	var date2_ms = date2.getTime();

	// Calculate the difference in milliseconds
	var difference_ms = date2_ms - date1_ms;
    
	// Convert back to days and return
	return Math.round(difference_ms/one_day); 
}

function TimeInDays(date) {
	return Math.round(date.getTime()/(1000*60*60*24));
}

function HashtagObject(tag_data, color) {
	this.hashtag = tag_data.hashtag;
	var tokens = tag_data.date.split("-");
	this.date = new Date(parseInt(tokens[2]), parseInt(tokens[1])-1, parseInt(tokens[0]));
	this.visible = false;
	// This is a circle object
	this.location = new google.maps.Circle({
				center : new google.maps.LatLng(tag_data.loc[0], tag_data.loc[1]),
				fillColor : color,
				radius : 10000,
				map: null
	});
}

// This function will draw all circles that are visible and within the specified
// time span on the map.
function Redraw() {
	for(i in Hashtags[legend_counter]) {
		for(j in Hashtags[legend_counter][i]) {
			if(Hashtags[legend_counter][i][j].visible == true && Hashtags[legend_counter][i][j].date >= FilterDate) {
				console.log("Redrawing " + Hashtags[legend_counter][i][j].hashtag + " because " + Hashtags[legend_counter][i][j].visible);
				Hashtags[legend_counter][i][j].location.setMap(Map);
				console.log(Hashtags[legend_counter][i][j].location)
			}
			else {
				console.log("Erasing " + Hashtags[legend_counter][i][j].hashtag + " because " + Hashtags[legend_counter][i][j].visible);
				Hashtags[legend_counter][i][j].location.setMap(null);
			}
		}
	}
}

// This function erases all points on the map
function ClearMap() {
	for(i in Hashtags[legend_counter]) {
		for(j in Hashtags[legend_counter][i]) {
		// TODO: Check the date
			if(Hashtags[legend_counter][i][j].visible == true){
				console.log("Erasing " + Hashtags[legend_counter][i][j].hashtag + " because " + Hashtags[legend_counter][i][j].visible);
				Hashtags[legend_counter][i][j].location.setMap(null);
				Hashtags[legend_counter][i][j].visible = false;
			}
		}
	}
}

// Remember the format of the raw data
// 2d array, 1st dimension is an array or arrays
// 2nd is any array of objects
function LoadHashtags(raw_data) {
	// loop through every array (groups of the same hashtag)
	// separates the hashtags into groups and creates the HashtagObjects
	for(i in raw_data) {
		var hashtag_group = new Array();
		for(j in raw_data[i]) {
			hashtag_group.push(new HashtagObject(raw_data[i][j], COLORS[i%5]));
		}
		
		// 0-5
		var index = Math.floor(i/5);
		if(i%5 == 0) {
			Hashtags[index] = new Array();
		}
		Hashtags[index].push(hashtag_group);
	}
}

// This function creates the checkbox legend after a query is made
// It also binds the click event to the checkboxes
function PopulateLegend() {
	var legend = "<form><p>Legend:</p>", i;
	var i, x=Hashtags[legend_counter].length;
	console.log(x);
	console.log(Hashtags[legend_counter]);
	for(i=0; i<x; i++) {
		console.log(legend_counter);
		legend = legend+ "<input type=\"checkbox\" class=\"filter\" name=\"hashtag\" value=\"" +
		Hashtags[legend_counter][i][0].hashtag +
		"\" /> #" + Hashtags[legend_counter][i][0].hashtag + " &nbsp\n";
	}
	legend = legend + "&nbsp<input type=\"button\" value=\"Previous\" id=\"previous\"><input type=\"button\" value=\"Next\" id=\"next\">";
	legend = legend + "</form>";
	$("#legend").html(legend);
	
	// Bind click events for the new forms
	$("input#next").click(function () {
		ClearMap();
		if(legend_counter+1 == Hashtags.length)
			legend_counter = 0;
		else
			legend_counter++;
		PopulateLegend();
	});
	
	$("input#previous").click(function () {
		ClearMap();
		console.log("length: " + Hashtags.length);
		if(legend_counter-1<0)
			legend_counter = Hashtags.length-1;
		else
			legend_counter--;
		PopulateLegend();
	});
	
	$("input:checkbox").click(function () {
		var tag = $(this).attr('value');
		for(i=0; i<x; i++) {
			if(Hashtags[legend_counter][i][0].hashtag == tag) {
				for(j in Hashtags[legend_counter][i]) {
					if(Hashtags[legend_counter][i][j].visible == true) {
						console.log(Hashtags[legend_counter][i][j].hashtag+" is invisible!");
						Hashtags[legend_counter][i][j].visible = false;
					}
					else {
						console.log(Hashtags[legend_counter][i][j].hashtag+" is visible!");
						Hashtags[legend_counter][i][j].visible = true;
					}
				}
				break;
			}
		}
		Redraw();
	});
}

function CreateSlider() {
	$(function () {
		console.log("Today: " + new Date());
		console.log("Query: " + StartDate);
		console.log("Difference: " +daysBetween(StartDate, new Date()));
		$("#timeline").slider({
			min: 0,
			max: TimeInDays(new Date())-TimeInDays(StartDate),
			slide: function( event, ui ) {
				FilterDate = new Date(StartDate.getTime()+ui.value*1000*60*60*24);
				Redraw();
			}
		});
	});
}



function Query() {
	var keywords = $("input#query").val();
	var startDate = $("input#startDate").val();
	console.log(keywords);
	var tokens = startDate.split("-");
	StartDate = new Date(parseInt(tokens[2]), parseInt(tokens[1])-1, parseInt(tokens[0]));
	FilterDate = new Date(parseInt(tokens[2]), parseInt(tokens[1])-1, parseInt(tokens[0]));
	console.log(StartDate);
	$.getJSON("http://localhost:8080/query/"+keywords+"/"+startDate, function(data, testStatus) {
		LoadHashtags(data.tags);
		PopulateLegend();
		CreateSlider();
	});
}


function Initialize() {
	$("#startDate").datepicker({dateFormat: "dd-mm-yy"});
	var myOptions = {
		center: new google.maps.LatLng( 0, 0),
		zoom: 1,
		mapTypeId: google.maps.MapTypeId.ROADMAP
	};
	Map = new google.maps.Map($("#map")[0],
		myOptions);
}

/*$("input.filter").click(function () {}*/

	