window.chartColors = {
      red: 'rgb(255, 99, 132)',
      orange: 'rgb(255, 159, 64)',
      yellow: 'rgb(255, 205, 86)',
      green: 'rgb(75, 192, 192)',
      blue: 'rgb(54, 162, 235)',
      purple: 'rgb(153, 102, 255)',
      grey: 'rgb(201, 203, 207)'
};


// Get the server address
function getServer() {
    let server = "";
    if (serverAddr != "") {
        server = serverAddr;
    } else {
        let protocol = location.protocol;
        let slashes = protocol.concat("//");
        server = slashes.concat(window.location.host);
    }
    return server;
};

// d3 dot vix attributor
function attributer(datum, index, nodes) {
    let selection = d3.select(this);
    if (datum.tag == "svg") {
        let width = window.innerWidth * 0.6;
        let height = window.innerHeight * 0.5;
        let x = 0;
        let y = 0;
        let scale = 0.3;
        selection
            .attr("width", width + "pt")
            .attr("height", height + "pt")
            .attr("viewBox", -x + " " + -y + " " + (width / scale) + " " + (height / scale));
        datum.attributes.width = width + "pt";
        datum.attributes.height = height + "pt";
        datum.attributes.viewBox = -x + " " + -y + " " + (width / scale) + " " + (height / scale);
    }
};

// updateGraph updates the graph
function updateGraph(dag) {
    d3.select("#graph")
        .graphviz()
        .tweenShapes(false)
        .attributer(attributer)
        .renderDot(dag);
};

// show a alert in dash-board
function triggerAlert(alertText, bsStyle) {
    document.getElementById("alert.container").innerHTML = '<div class="alert alert-' + bsStyle + ' fade show">' +
        '<button type="button" class="close" data-dismiss="alert"> &times;</button>' + alertText + '</div>';
};

// Load the trace content async and periodic manner
function loadTraceContent(flowName, reqId, traceId) {
    let url = getServer();
    url = url.concat("/api/flow/request/traces");

    let reqData = {};
    reqData["function"] = flowName;
    reqData["trace-id"] = traceId;
    reqData["request-id"] = reqId;
    let data = JSON.stringify(reqData);

    let xmlHttp = new XMLHttpRequest();
    xmlHttp.onreadystatechange = function () {
        if (this.readyState == 4 && this.status != 200) {
            triggerAlert("Failed to get flow details; " + this.responseText, 'danger');
            return;
        }
        if (this.readyState == 4 && this.status == 200) {
            updateTraceContent(JSON.parse(this.responseText));
        }
    };
    xmlHttp.open("POST", url, true);
    xmlHttp.setRequestHeader('accept', "application/json");
    xmlHttp.setRequestHeader("Content-Type", "application/json");
    xmlHttp.send(data);
};

// execute the flow function
function executeFlow(flowName) {
    let url = getServer();
    url = url.concat("/api/flow/request/execute");

    let reqData = {};
    reqData["function"] = flowName;
    reqData["data"] = document.getElementById("request.body").value;
    let data = JSON.stringify(reqData);
    let contentType = document.querySelector('input[name="request.content_type"]:checked').value;

    let xmlHttp = new XMLHttpRequest();
    xmlHttp.onreadystatechange = function () {
        if (this.readyState == 4) {
            let requestId = this.responseText;
            document.getElementById("response.status").value = "" + this.status;
            if (requestId != null) {
                document.getElementById("response.id").value = requestId;
                document.getElementById("response.monitor").style.visibility = "visible";
                document.getElementById("response.monitor").href =
                    getServer().concat("/flow/request/monitor?flow-name="
                        + flowName + "&request=" + requestId);
            } else {
                document.getElementById("response.monitor").style.visibility = "hidden";
            }
        }
    };
    xmlHttp.open("POST", url, true);
    xmlHttp.setRequestHeader("Content-Type", contentType);
    xmlHttp.send(data);
};

// stop the request
function stopRequest(flowName, request) {
    let url = getServer();
    url = url.concat("/api/flow/request/stop");

    let reqData = {};
    reqData["function"] = flowName;
    reqData["request-id"] = request;

    let data = JSON.stringify(reqData);
    let contentType = "application/json";

    let html = document.getElementById("exec-status").innerHTML;
    if (!(html.includes("RUNNING") || html.includes("PAUSED"))) {
        triggerAlert("Can't stop request: <b>" + request + "</b>, must be in <b>ACTIVE</b> states", 'info');
        return;
    }

    let xmlHttp = new XMLHttpRequest();
    xmlHttp.onreadystatechange = function () {
        if (this.readyState == 4 && this.status != 200) {
            triggerAlert("Failed to stop the request: <b>" + request + "</b>", 'danger');
            return;
        }
        if (this.readyState == 4 && this.status == 200) {
            triggerAlert("Request: <b>" + request + "</b> has been stopped", 'success');
            return;
        }
    };

    xmlHttp.open("POST", url, true);
    xmlHttp.setRequestHeader("Content-Type", contentType);
    xmlHttp.send(data);
};

// pause the request
function pauseRequest(flowName, request) {
    let url = getServer();
    url = url.concat("/api/flow/request/pause");

    let reqData = {};
    reqData["function"] = flowName;
    reqData["request-id"] = request;

    let data = JSON.stringify(reqData);
    let contentType = "application/json";

    let html = document.getElementById("exec-status").innerHTML;
    if (!(html.includes("RUNNING"))) {
        triggerAlert("Can't stop request: <b>" + request + "</b>, must be in <b>RUNNING</b> states", 'info');
        return;
    }

    let xmlHttp = new XMLHttpRequest();
    xmlHttp.onreadystatechange = function () {
        if (this.readyState == 4 && this.status != 200) {
            triggerAlert("Failed to pause the request: <b>" + request + "</b>", 'danger');
            return;
        }
        if (this.readyState == 4 && this.status == 200) {
            triggerAlert("Request: <b>" + request + "</b> has been paused!", 'success');
            return;
        }
    };

    xmlHttp.open("POST", url, true);
    xmlHttp.setRequestHeader("Content-Type", contentType);
    xmlHttp.send(data);
};

// resume the request
function resumeRequest(flowName, request) {
    let url = getServer();
    url = url.concat("/api/flow/request/resume");

    let reqData = {};
    reqData["function"] = flowName;
    reqData["request-id"] = request;

    let data = JSON.stringify(reqData);
    let contentType = "application/json";

    let html = document.getElementById("exec-status").innerHTML;
    if (!html.includes("PAUSED")) {
        triggerAlert("Can't resume request: <b>" + request +"</b>, must be <b>PAUSED</b>", 'info');
        return;
    }

    let xmlHttp = new XMLHttpRequest();
    xmlHttp.onreadystatechange = function () {
        if (this.readyState == 4 && this.status != 200) {
            triggerAlert("Failed to resume the request: <b>" + request + "</b>", 'danger');
            return;
        }
        if (this.readyState == 4 && this.status == 200) {
            triggerAlert("Request: <b>" + request + "</b> has been resumed", 'success');
            return;
        }
    };

    xmlHttp.open("POST", url, true);
    xmlHttp.setRequestHeader("Content-Type", contentType);
    xmlHttp.send(data);
};


/*
// delete the flow function
function deleteFlow(flowName) {
    $('#deleteModal').modal('hide');

    let url = getServer();
    url = url.concat("/function/" + flowName + "?resume-flow=" + request);

    let html = document.getElementById("exec-status").innerHTML;
    if (!html.includes("PAUSED")) {
        triggerAlert("Can't resume request: <b>" + request +"</b>, must be <b>PAUSED</b>", 'info');
        return;
    }

    let xmlHttp = new XMLHttpRequest();
    xmlHttp.onreadystatechange = function () {
        if (this.readyState == 4 && this.status != 200) {
            triggerAlert("Failed to delete flow: <b>" + flowName + "</b>", "danger");
            return;
        }
        if (this.readyState == 4 && this.status == 200) {
            triggerAlert("Deleted flow: <b>" + flowName + "</b>", "success");
            return;
        }
    };

    xmlHttp.open("POST", url, true);
    xmlHttp.setRequestHeader("Content-Type", contentType);
    xmlHttp.send(data);
};
*/

// format function duration in sec
function formatDuration(micros) {
    let seconds = (micros / 1000000);
    return "" + seconds + "s";
};

// format Time in hour:min:sec
function formatTime(unix_timestamp) {
    let date = new Date(unix_timestamp/1000);
    let hours = date.getHours();
    let minutes = "0" + date.getMinutes();
    let seconds = "0" + date.getSeconds();

    // Will display time in 10:30:23 format
    let formattedTime = hours + ':' + minutes.substr(-2) + ':' + seconds.substr(-2);
    return formattedTime
};

// draw the bar chart for request tarces
function drawBarChart(jsonObject) {
    let id = jsonObject["request-id"];
    let rstime = jsonObject["start-time"];
    let rduration = jsonObject["duration"];
    let traces = jsonObject["traces"];
  
    let container = document.getElementById('canvas');
    let chart = new google.visualization.Timeline(container);
    let dataTable = new google.visualization.DataTable();


    dataTable.addColumn({ type: 'string', id: 'ID' });
    dataTable.addColumn({ type: 'number', id: 'Start' });
    dataTable.addColumn({ type: 'number', id: 'End' });
    
    let rows = [];

    let normalizer = 1000;
    let requestdata = [id, (rstime/normalizer), ((rstime+rduration)/normalizer)];
    rows.push(requestdata);

    for (let node in traces) {
        let value = traces[node];
        let nstime = value["start-time"];
        let nduration = value["duration"];
        rows.push([node, nstime/normalizer, ((nstime+nduration)/normalizer)]);
    }
    dataTable.addRows(rows)

    let options = {
        animation:{
            duration: 1000,
            easing: 'out',
        },
	    hAxis:{
	        minValue: (rstime/normalizer),
	        maxValue: ((rstime+rduration)/normalizer),
	    },
    };
    chart.draw(dataTable, options);
};

// Update the content of content wrapper for request desc
function updateTraceContent(jsonObject) {

    let duration = jsonObject["duration"];
    let status = jsonObject["status"];
    let start_time = jsonObject["start-time"];

    // remove welcome body if present
    welcome = d3.select("#welcome")
    if (welcome !== null ) {
        welcome.remove();
    }

    // draw the bar chart
    google.charts.load("current", {
	    packages: ["timeline"]},
    );
    google.charts.setOnLoadCallback(function(){
	    drawBarChart(jsonObject);
    });

    document.getElementById("exec-duration").innerHTML = "<b>Duration:</b> " + formatDuration(duration);
    document.getElementById("exec-status").innerHTML = "<b>Status:</b> " + status;
    document.getElementById("start-time").innerHTML = "<b>Start Time:</b> " + formatTime(start_time);
};



