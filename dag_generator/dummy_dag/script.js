var main_form = document.getElementById("main_form");
        
var form_list = document.getElementsByTagName("form");

var url_request = new URL('/dummydagappbuilderbaseview?', document.baseURI);
var url_request_params = new URLSearchParams(url_request);

//Override default form submit behaviour
last_form.onsubmit = function(event) {
    event.preventDefault();

    var main_form_data = new FormData(main_form);

    for (var pair of main_form_data.entries()) {
        console.log(pair[0] + ' - ' + pair[1]); 
        url_request_params.append(pair[0], pair[1]);
    }

    // Forms before three are forms before task groups
    // Last form is generation button
    for (var i = 3; i < form_list.length - 1; i++) {
        var task_group_form_data = new FormData(form_list[i]);

        for (var pair of task_group_form_data.entries()) {
            console.log(pair[0] + ' - ' + pair[1]); 
            url_request_params.append(pair[0], pair[1]);
        }                
    }    
    
    //Check if inputs are valid
    if (validateDAG()) {
        url_request_params.append("form_btn", "gen_dag_btn");

        console.log(url_request.toString()+url_request_params.toString());

        fetch_url(url_request, url_request_params);
    }
    else {
        console.log("Not Valid");
    }

    return false;
}

function validateDAG() {
    var dag_domain_input = document.getElementById("dag_domain");
    var dag_name_input = document.getElementById("dag_name");

    var scheduler_input = document.getElementById("interval_text"); 

    var dag_domain_value = dag_domain_input.value;
    var dag_name_value = dag_name_input.value;

    var scheduler_value = scheduler_input.value;

    var form_list = document.getElementsByTagName("form");
    
    if (dag_domain_value == null || dag_domain_value == "") 
    {
        alert("Invalid DAG Domain Input!");
        return false;
    }

    if (dag_name_value == null || dag_name_value == "") 
    {
        alert("Invalid DAG Name Input!");
        return false;
    }

    if (scheduler_value != "None")
    {
        var scheduler_splitted = scheduler_value.split(" ");

        if (scheduler_splitted.length != 5)
        {
            alert("Invalid Scheduler Input!");
            return false;
        }
    }

    for (var i = 3; i < form_list.length - 1; i++) {
        var task_group_form_data = new FormData(form_list[i]);
        var db_count_other_flag = false;
        
        for (var pair of task_group_form_data.entries()) {
            if (pair[0] == "task_group_name_text" && pair[1] == ""){
                alert("Invalid Task Group Name Input!");
                return false;
            }
            if (pair[0] == "db_names_list" && pair[1] == ""){
                alert("Invalid Data Base Names Input!");
                return false;
            }
            if (pair[0] == "db_count_sensor" && pair[1] == "other"){
                db_count_other_flag = true;
            }                    
            if (db_count_other_flag && pair[0] == "db_count_sensor_other" && pair[1] == ""){
                alert("Invalid Data Base Select Count Input!");
                return false;
            }
        }
    }            

    return true;
}

function createElements(htmlStr) {
    var frag = document.createDocumentFragment(),
    temp = document.createElement('div');
    temp.innerHTML = htmlStr;
    while (temp.firstChild) {
        frag.appendChild(temp.firstChild);
    }
    return frag;
}

function createTaskGroup() {
    console.log("New Task Group created!");

    var actual_body = document.querySelector(".row");

    var htmlStrTaskGroupName = '<div class="container"><div class="col-xs-2"><label for="task_group_name_text">Task Group Name:</label><input class="form-control" name="task_group_name_text" id="task_group_name_text"></div></div>';
    var dbNames = '<div class="container"><div class="col-xs-6"><label for="db_names_list">Data Bases Names (Separate names with ,):</label><input class="form-control" name="db_names_list" id="db_names_list" placeholder="schema.db_name1, schema.db_name2"></div></div>';
    var dbCountRadio = '<div class="container"><div class="col-xs-5"><label>How many DBs should be at least selected:</label><div><label><input type="radio" name="db_count_sensor" id="db_count_sensor" value="1" checked="checked"> One</label></div><div><label><input type="radio" name="db_count_sensor" id="db_count_sensor" value="all"> All</label></div><div class="input-group"><span class="input-group-addon"><input type="radio" name="db_count_sensor" id="db_count_sensor" value="other"></span><input type="text" name="db_count_sensor_other" id="db_count_sensor_other" class="form-control"></div></div></div>';
    var divider = '<hr>'

    var new_form_template = '<form id="radio_form"></form>'

    var new_form_fragment = createElements(new_form_template);
    var last_form = document.getElementById("last_form");

    var fragment_taskGroupName = createElements(htmlStrTaskGroupName);
    var fragment_dbNames = createElements(dbNames);
    var fragment_dbCountRadio = createElements(dbCountRadio);
    var fragment_divider = createElements(divider);

    actual_body.insertBefore(new_form_fragment, last_form);

    var new_form_list = document.getElementsByTagName("form");
    var last_new_form = new_form_list[new_form_list.length-2];

    last_new_form.appendChild(fragment_taskGroupName);
    last_new_form.appendChild(fragment_dbNames);
    last_new_form.appendChild(fragment_dbCountRadio);
    last_new_form.appendChild(fragment_divider);
}

async function fetch_url(url_arg, params_arg) {
    try{
        const response = await fetch(url_arg.toString()+params_arg.toString());
        if (!response.ok) {
            throw new Error(`Response status: ${response.status}`);
        }
        if (response.redirected) {
            window.location.href = response.url;
            return;
        }
    }
    catch (error) {
        console.error(error.message);
    }
}