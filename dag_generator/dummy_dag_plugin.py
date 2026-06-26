# This is the class you derive to create a plugin
from airflow.plugins_manager import AirflowPlugin
from airflow.security import permissions
from airflow.www.auth import has_access

from flask import Blueprint
from flask import Flask, request, render_template 
from flask import redirect, url_for
from flask import make_response, send_file
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from validate_cron import _validate_cron

import os
import logging
from pathlib import Path
from string import Template
import time

import zipfile

# Creating a flask blueprint to integrate the templates and static folder
bp = Blueprint(
    "dummy_dag_plugin",
    __name__,
    template_folder="templates",  # registers airflow/plugins/templates as a Jinja template folder
    static_folder="static",
    static_url_path="/static/dummy_dag_plugin",
)

BASE_DIR = "/opt/airflow/plugins/dummy_dags_storage"
SAVE_DIR = "/opt/airflow/dummy_dags_storage"

# Placeholder values for data
template_values = {
    'dag_name': "temp",
    'dag_db_connection': "greenplum",
    'dag_tags': ["example", "ui"],
    'dag_interval': None,
    'dag_depends_on_past': False,
    'task_group_name': "task_group_name",
    'db_names_list': "",
    'db_count_sensor': "all"
}

# Creating a flask appbuilder BaseView
class DummyDagAppBuilderBaseView(AppBuilderBaseView):
    default_view = "dummy_dag"

    @expose("/")
    @has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ]
    )
    def dummy_dag(self):
        # Read all the data from request
        btn_type = request.args.get('form_btn', '')
        dag_name = request.args.get('dag_name', '')
        dag_domain = request.args.get('dag_domain', '')
        dag_db = request.args.get('db_dropdown', '')
        dag_layer = request.args.get('layer_dropdown', '')
        dag_tags = request.args.getlist('tag_check')
        dag_interval = request.args.get('interval_text', '')
        task_group_name = request.args.getlist('task_group_name_text')
        db_names_list = request.args.getlist('db_names_list')
        db_count_sensor = request.args.getlist('db_count_sensor')
        db_count_sensor_other = request.args.getlist('db_count_sensor_other')

        # Placeholder values
        dag_depends_on_past = False
        dag_id = ''
        dag_db_connection = ''
        dag_db_id = dag_db

        result_output = ""
        task_group_output = ""

        # DAG names and domains must be in upper-case
        dag_name = dag_name.upper()
        dag_domain = dag_domain.upper()

        # Check if save directory exists. If not -> create it
        if not os.path.exists(BASE_DIR):
            os.makedirs(BASE_DIR, exist_ok=True)

        if (dag_db_id == 'GP_C') or (dag_db_id == 'GP_P'):
            dag_db_id = 'GP'
            dag_db_connection = "'greenplum'"
        else:
            dag_db_connection = "'clickhouse'"

        # If "Generate DAG" button is pressed
        if btn_type == 'gen_dag_btn':
            time_name = str(time.strftime("%Y%m%d-%H%M%S")) + ".py"

            # Generate DAG ID out of provided data
            dag_id = "'" + "LOAD_" + dag_db_id + "_" + dag_layer + "_" + dag_domain + "_"  + dag_name + "'"

            # If there is _im tag present - depends_on_past should be true
            if '_im' in dag_tags:
                dag_depends_on_past = True

            # Validate time interval
            if dag_interval != 'None':
                if _validate_cron(dag_interval) != None:
                    dag_interval = 'None'
                else:
                    dag_interval = "'" + dag_interval + "'"

            template_values['dag_id'] = dag_id
            template_values['dag_db_connection'] = dag_db_connection
            template_values['dag_tags'] = dag_tags
            template_values['dag_interval'] = dag_interval
            template_values['dag_depends_on_past'] = dag_depends_on_past

            with open(os.path.join(BASE_DIR, 'DAG_TEMPLATE.txt'), 'r') as dag_template:
                src = Template(dag_template.read())
                result_output = src.substitute(template_values)

            with open(os.path.join(BASE_DIR, 'TASK_GROUP_TEMPLATE.txt'), 'r') as task_group_template:
                src = Template(task_group_template.read())

                for i in range(0, len(task_group_name)):
                    print("loop num is " + str(i))
                    template_values['task_group_name'] = task_group_name[i]

                    # If 'other' is set - read user provided data
                    if db_count_sensor[i] == 'other':
                        db_count_sensor[i] = db_count_sensor_other[i]
                    
                    template_values['db_names_list'] = db_names_list[i]
                    template_values['db_count_sensor'] = db_count_sensor[i]

                    temp = src.substitute(template_values)

                    task_group_output = task_group_output + temp

            # Place Task Groups inside generated DAG
            result_output = result_output.replace("%place_for_task_group%", task_group_output)
            
            # Save generated files inside zip archive
            try:
                with open(os.path.join(SAVE_DIR, time_name), "x", encoding="utf-8") as dag_output:
                    dag_output.write(result_output)
                with zipfile.ZipFile(os.path.join(SAVE_DIR, time_name + ".zip"), 'w') as files_zip:
                        files_zip.write(os.path.join(SAVE_DIR, time_name), os.path.join("dags", time_name))

                # Return view for downloading generated archive
                return redirect("downloadnewdag/" + time_name + '.zip')
            except FileExistsError:
                print("File already exists!")

        return self.render_template(
            "dummy_dag/dummy_dag.html", 
            dag_id=dag_id, 
            interval_text=dag_interval, 
            form_btn=btn_type,
            tag_check=dag_tags
        )

    # View for downloading generated DAG
    @expose("/downloadnewdag/<string:dag_filename>")
    def return_file(self, dag_filename):
        try:
            file_path = os.path.join(SAVE_DIR, dag_filename)

            # Return file if it exists. Return 404 if it doesn't exist
            if os.path.isfile(file_path):
                return send_file(file_path, as_attachment=True)
            else:
                return make_response(f"File '{dag_filename}' not found.", 404)
        except Exception as e:
            return make_response(f"Error: {str(e)}", 500)


v_appbuilder_view = DummyDagAppBuilderBaseView()
v_appbuilder_package = {
    "name": "Dummy Dag",
    "category": "Dummy Dag",
    "view": v_appbuilder_view,
}


# Defining the plugin class
class AirflowDummyDagPlugin(AirflowPlugin):
    name = "dummy_dag_plugin"
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]