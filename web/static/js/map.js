$(document).ready(Initialize);
$("input#search").click(Query);

// This is going to be the  array containing EventObjects
var Events = new Array();
var COLORS = ["red", "blue", "yellow", "green", "black"];
var legend_counter = 0;
var current_event, Map, StartDate, EndDate, FilterStartDate, FilterEndDate, slider, Span;

function daysBetween ( date1, date2 ) {
	//Get 1 day in milliseconds
	var one_day=1000*60*60*24;

	// Convert both dates to milliseconds
	var date1_ms = date1.getTime();
	var date2_ms = date2.getTime();

	// Calculate the difference in milliseconds
	var difference_ms = Math.abs(date2_ms - date1_ms);
    
	// Convert back to days and return
	return Math.round(difference_ms/one_day); 
}

function TimeInDays(date) {
	return Math.round(date.getTime()/(1000*60*60*24));
}

// Converted***********************************************************************
function EventObject(event_data, color) {
	this.event = event_data;
	//var tokens = event_data..date.split("T");
	//var d = tokens[0].
	//this.date = new Date(parseInt(tokens[2]), parseInt(tokens[1])-1, parseInt(tokens[0]));
	this.visible = false;
	// This is a circle object
	this.locations = new Array();
	for (i in this.event.tweets) {
		this.locations.push(
			new google.maps.Circle({
			center : new google.maps.LatLng(this.event.tweets[i].location.lat, this.event.tweets[i].location.lng),
			fillColor : color,
			radius : 16000,
			map: null
		}));
	}
}

// Converted******************************************************
// This function will draw all circles that are visible and within the specified
// time span on the map.
function Redraw() {
	for (i in Events) {
		if (Events[i].visible == true ){//&& Hashtags[legend_counter][i][j].date >= FilterStartDate && Hashtags[legend_counter][i][j].date <= FilterEndDate) {
			console.log("Redrawing " + Events[i].event.id + " because " + Events[i].visible);
			for (j in Events[i].locations){
				Events[i].locations[j].setMap(Map);
			}
		}
		else {
			for (j in Events[i].locations){
				Events[i].locations[j].setMap(null);
			}
		}
	}
}

// Converted***************************************************
// This function erases all points on the map
function ClearMap() {
	for(i in Events) {
		// TODO: Check the date
		if(Events[i].visible == true){
			for (j in Events[i].locations){
				Events[i].locations[j].setMap(null);
			}
			Events[i].visible = false;
		}
	}
}

// Converted******************************************************
// raw_data is a list of 5 EventObjects
function LoadEvents(events) {
	//console.log(events)
	Events = new Array();
	for(i in events) {
		Events.push(new EventObject(events[i], COLORS[i]));
	}
}

// This function will populate the summary field with tweets and cluster stats
function PopulateSummary(eventObj) {
	var event_stats = "<div id=\"event_stats\"><p>Size of event: " + eventObj.event.tweets.length + "<br>";
	var tweets = "<div id=\"tweet\">";
	for(i=0; i<eventObj.event.tweets.length; i++) {
		var tweetObj = eventObj.event.tweets[i];
		tweets = tweets + "<p>User ID: " + tweetObj._id + "<br>" + tweetObj.contents + "<br> Similarity to event centroid: " + eventObj.event.similarities[i]+"<br></p>";	
	}
	tweets = tweets  + "</div>";
	event_stats = event_stats + tweets;
	
	event_stats = event_stats + "</p></div>"
	$("#summaries").html(event_stats)
}

// Converted******************************************************
// This function creates the checkbox legend after a query is made
// It also binds the click event to the checkboxes
function PopulateLegend() {
	var legend = "<form><p>Legend:</p>", i;
	console.log(Events);
	for(i=0; i<Events.length; i++) {
		//console.log(Events[i]);
		legend = legend+ "<input type=\"radio\" name=\"event\" value=\"" +
		Events[i].event.id +
		"\" /> <b id=\"filter"+i+"\">Event</b>" + Events[i].event.id + "&nbsp\n";
	}
	legend = legend + "&nbsp<input type=\"button\" value=\"Previous\" id=\"previous\"><input type=\"button\" value=\"Next\" id=\"next\">";
	legend = legend + "</form>";
	$("#legend").html(legend);
	for(i=0; i<Events.length; i++) {
		$("#filter"+i).css("color",COLORS[i]);
		$("#filter"+i).css("font-weight","bold");
	}
	
	// Bind click events for the new forms
	$("input#next").click(function () {
		ClearMap();
		// Get the next 5 events from the server
		RequestEvents(1);
		PopulateLegend();
	});
	
	$("input#previous").click(function () {
		ClearMap();
		// Get the previous 5 events from the server
		RequestEvents(-1);
		PopulateLegend();
	});
	
	$("input:radio").click(function () {
		ClearMap();
		current_event = $(this).attr('value');
		for(i=0; i<Events.length; i++) {
			if(Events[i].event.id == current_event) {
				console.log(Events[i].event.id+" is visible!");
				Events[i].visible = true;
				PopulateSummary(Events[i]);
			}
		}
		Redraw();
	});
}

// Ignored for now
/*
function CreateSlider() {
	$("#timeline").html("<div id=\"slider\"></div>")
	$(function () {
		console.log("Today: " + new Date());
		console.log("Query: " + StartDate);
		console.log("Difference: " +daysBetween(StartDate, new Date()));
		$("#slider").slider({
			min: 0,
			max: TimeInDays(new Date())-TimeInDays(StartDate),
			value: 0,
			animate: "slow",
			slide: function( event, ui ) {
				FilterStartDate = new Date(StartDate.getTime()+ui.value*1000*60*60*24);
				FilterEndDate = new Date(FilterStartDate.getTime()+Span*1000*60*60*24);
				Redraw();
			}
		});
	});
}
*/

// Converted********************************************************************
function Query() {
	ClearMap();
	// This will be a list of 5 events from the first date in the query span
	Events = new Array();
	var QueryStartDate = $("input#startDate").val();
	var QueryStartTime = $("input#startTime").val().toString();
	var QueryEndDate = $("input#endDate").val();
	var QueryEndTime = $("input#endTime").val().toString();
	var date = QueryStartDate.split("-");
	var time = QueryStartTime.split(":");
	StartDate = new Date(parseInt(date[2]), parseInt(date[1])-1, parseInt(date[0]), parseInt(time[0]), parseInt(time[1]));
	FilterStartDate = new Date(parseInt(date[2]), parseInt(date[1])-1, parseInt(date[0]), parseInt(time[0]), parseInt(time[1]));
	
	var date = QueryEndDate.split("-");
	var time = QueryEndTime.split(":");
	FilterEndDate =  new Date(parseInt(date[2]), parseInt(date[1])-1, parseInt(date[0]), parseInt(time[0]), parseInt(time[1]));
	
	console.log(FilterStartDate);
	console.log(FilterEndDate);
	
	$.ajax("http://localhost:8080/QueryEvents/"+QueryStartDate+"/"+QueryStartTime+"/"+QueryEndDate+"/"+QueryEndTime,{
		timeout:15000000,
		success: function(data) {
			console.log(data);
			LoadEvents(data.events);
			if(Events.length == 0)
				alert("No results found");
			else {
				PopulateLegend();
				//CreateSlider();
			}
		},
		error: function(jqXHR,textStatus,errorThrown) {
			var error;
			if(textStatus=='error') {
				if(jqXHR.status==0)
					error = "Could not connect to server. Try running ./serve.py.";
				else
					error = jqXHR.status+" : "+errorThrown;
			} else {
				error = textStatus;
				console.log(error);
			}

			//var alert = alert_template({error:error});
			//console.log(alert);
		},
		dataType: 'json',
	});
	/*
	$.getJSON("http://localhost:8080/QueryEvents/"+QueryStartDate+"/"+QueryEndDate, function(data, testStatus) {
		// initially data is the first 5 events detected in the first day within the query span
		// ********************NOTE Under Construction**************
		console.log(testStatus);
		console.log(data);
		LoadEvents(data.events);
		if(Events.length == 0)
			alert("No results found");
		else {
			PopulateLegend();
			//CreateSlider();
		}
	});*/
}

function RequestEvents(dir) {
	$.ajax("http://localhost:8080/RequestEvents/"+dir,{
		timeout:1500000000000,
		success: function(data) {
			console.log(data);
			LoadEvents(data.events);
			if(Events.length == 0)
				alert("No results found");
			else {
				PopulateLegend();
				//CreateSlider();
			}
		},
		error: function(jqXHR,textStatus,errorThrown) {
			var error;
			if(textStatus=='error') {
				if(jqXHR.status==0)
					error = "Could not connect to server. Try running ./serve.py.";
				else
					error = jqXHR.status+" : "+errorThrown;
			} else {
				error = textStatus;
				console.log(error);
			}

			//var alert = alert_template({error:error});
			//console.log(alert);
		},
		dataType: 'json',
	});
}

function Initialize() {
	$("#startDate").datepicker({dateFormat: "dd-mm-yy"});
	$("#endDate").datepicker({dateFormat: "dd-mm-yy"});
	$.widget( "ui.timespinner", $.ui.spinner, {
		options: {
		  // 15 minutes
		  step: 15*60 * 1000,
		  // hours
		  page: 4
		},
 
		_parse: function( value ) {
		  if ( typeof value === "string" ) {
			// already a timestamp
			if ( Number( value ) == value ) {
			  return Number( value );
			}
			return +Globalize.parseDate( value );
		  }
		  return value;
		},
 
		_format: function( value ) {
		  return Globalize.format( new Date(value), "t" );
		}
	});
	Globalize.culture('de-DE');
	$("#startTime").timespinner();
	$("#endTime").timespinner();
	$( "#startTime" ).timespinner( "value", "00:00");
	$( "#endTime" ).timespinner( "value", "00:00");
	
	var myOptions = {
		center: new google.maps.LatLng( 38, -95),
		zoom: 4,
		disableDefaultUI: true,
		mapTypeId: google.maps.MapTypeId.ROADMAP
	};
	Map = new google.maps.Map($("#map")[0],
		myOptions);
}
