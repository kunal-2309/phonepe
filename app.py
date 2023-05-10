from elasticsearch import Elasticsearch
from flask import Flask, jsonify, request, render_template, redirect, url_for
from datetime import datetime
import json
import logging
from logging.handlers import RotatingFileHandler
es = Elasticsearch(["http://192.168.64.6:9200","http://192.168.64.7:9200","http://192.168.64.5:9200"])

logger = logging.getLogger()

# create a formatter object
logFormatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")

# add console handler to the root logger
consoleHanlder = logging.StreamHandler()
consoleHanlder.setFormatter(logFormatter)
logger.addHandler(consoleHanlder)

# add file handler to the root logger
fileHandler = logging.FileHandler("logs.log")
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

app = Flask(__name__)


# index_settings = {
#     "settings": {
#         "number_of_replicas": 2
#     },
#     "mappings": {
#         "properties": {
#             "Employee_Name": {
#                 "type": "text"
#             },
#             "Phone": {
#                 "type": "text"
#             },
#             "Start_Date": {
#                 "type": "date"
#             },
#             "End_Date": {
#                 "type": "date"
#             },
#             "Email": {
#                 "type": "text"
#             },
#             "Employee_Id": {
#                 "type": "text"
#             },
#             "Team_Name": {
#                 "type": "text"
#             },
#             "Role": {
#                 "type": "text"
#             }
#         }
#     }
# }


INDEX_NAME = 'users'


@app.route("/")
def index():
    return render_template('index.html')


@app.route('/users', methods=['GET', 'POST'])
def create_user():
    if request.method == 'GET':
        return render_template('home.html')
    if request.method == 'POST':
        Employee_Name = request.form['Employee_Name']
        # Employee_Name = request.form.get('Employee_Name', None)
        Start_Date = request.form['Start_Date']
        End_Date = request.form['End_Date']
        Role = request.form['Role']
        Email = request.form['Email']
        Phone = request.form['Phone']
        Team_Name = request.form['Team_Name']
        Employee_Id = request.form['Employee_Id']

        document = {
            'Employee_Name': Employee_Name,
            'Phone': Phone,
            'Start_Date': Start_Date,
            "End_Date": End_Date,
            "Employee_Id": Employee_Id,
            "Team_Name": Team_Name,
            "Role": Role,
            "Email": Email
        }
        try:
            result = es.index(index=INDEX_NAME, body=document)
            return render_template('success.html', id=result['_id'])
        except:
            return render_template('error.html')
# Read a document


@app.route('/users/get_user', methods=['GET', 'POST'])
def get_user():

    if request.method == "POST":
        start_date = datetime.strptime(request.form['Start_Date'], '%Y-%m-%d')
        end_date = datetime.strptime(request.form['End_Date'], '%Y-%m-%d')
        query = {



            "query": {
                "bool": {
                    "should": [
                        {
                            "bool": {
                                "must": [
                                    {
                                        "range": {
                                            "Start_Date": {
                                                "lte": end_date
                                            }
                                        }
                                    },
                                    {
                                        "range": {
                                            "End_Date": {
                                                "gte": start_date
                                            }
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "bool": {
                                "must": [
                                    {
                                        "range": {
                                            "Start_Date": {
                                                "gte": start_date,
                                                "lte": end_date
                                            }
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "bool": {
                                "must": [
                                    {
                                        "range": {
                                            "End_Date": {
                                                "gte": start_date,
                                                "lte": end_date
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        }
        try:
            result = es.search(index='users', body=query)
            hits = result['hits']['hits']
            # return result['hits']['hits']
            return render_template('user.html', hits=hits)
        except:
            return render_template('error.html')
    else:
        return render_template('user.html')
# Update a document


@app.route('/users/update_user', methods=['POST', 'GET'])
def update_user():
    if request.method == 'GET':
        return render_template('eid.html')
    if request.method == 'POST':
        Employee_Name = request.form['Employee_Name']
        Start_Date = request.form['Start_Date']
        End_Date = request.form['End_Date']
        Role = request.form['Role']
        Email = request.form['Email']
        Phone = request.form['Phone']
        Team_Name = request.form['Team_Name']
        Employee_Id = request.form['Employee_Id']
        document = {
            'Employee_Name': Employee_Name,
            'Phone': Phone,
            'Start_Date': Start_Date,
            "End_Date": End_Date,
            "Employee_Id": Employee_Id,
            "Team_Name": Team_Name,
            "Role": Role,
            "Email": Email
        }
    try:
        id = request.form['id']
        result = es.update(index=INDEX_NAME, id=id, body={'doc': document})
        return "Successfully updated"
    except:
        return render_template('error.html')


@app.route('/update', methods=['POST'])
def updater():
    Employee_Id = request.form['Employee_Id']
    query = {
        "query": {
            "match": {
                "Employee_Id": Employee_Id
            }
        }
    }
    try:
        result = es.search(index='users', body=query)
        if result['hits']['total']['value'] == 0:
            return "Record not found"
        else:
            result = dict(result)
            id = result['hits']['hits'][0]['_id']
            return render_template('update.html', data=result['hits']['hits'][0]['_source'], id=id)
    except:
        return render_template('error.html')
# Delete a document


@app.route('/users/delete_user', methods=['GET', 'POST'])
def delete_user():
    # if request.method=="POST":
    if request.method == 'POST':
        try:
            id = request.form['id']
            result = es.delete(index=INDEX_NAME, id=id)
            return "deleted successfully"
        except:
            return render_template('error.html')
    else:
        return render_template('delete.html')


@app.route('/delete', methods=['POST', 'DELETE'])
def deleter():
    Employee_Id = request.form['Employee_Id']
    query = {
        "query": {
            "match": {
                "Employee_Id": Employee_Id
            }
        }
    }
    try:
        result = es.search(index='users', body=query)
        if result['hits']['total']['value'] == 0:
            return "Record not found"
        else:
            result = dict(result)
            id = result['hits']['hits'][0]['_id']
            es.delete(index='users', id=id)
            return "deleted successfully"
            # return render_template('deleted.html', data=result['hits']['hits'][0]['_source'], id=id)
    except:
        return render_template('error.html')

def search_employee(employee_id):
    try:
        query = {
            "query": {
                "match": {
                    "Employee_Id": employee_id
                }
            }
        }
        result = es.search(index="users", body=query)
        return result['hits']['hits']
    except:
        return render_template('error.html')


@app.route('/search', methods=['GET', 'POST'])
def search():
    if request.method == 'POST':
        # Get employee ID from form input
        employee_id = request.form['emp_id']

        # Search for records matching employee ID
        results = search_employee(employee_id)

        if len(results) == 0:
            # No records found
            return 'No results found'
        else:
            # Display search results in HTML table
            return render_template('search_results.html', results=results)
    else:
        # Display search form
        return render_template('search.html')


@app.route('/update/<string:id>', methods=['GET', 'POST'])
def newupdate(id):
    # Get record from Elasticsearch by ID
    try:
        record = es.get(index='users', id=id)
    except:
        return render_template('error.html')

    if request.method == 'POST':
        # Update record with form data
        es.update(index='users', id=id, body={'doc': {
            'Employee_Name': request.form['emp_name'],
            'Role': request.form['role'],
            'Phone': request.form['phone'],
            'Email': request.form['email'],
            'Start_Date': request.form['start_date'],
            'End_Date': request.form['end_date'],
            'Team': request.form['Team']
        }})

        # Redirect to search page
        return redirect(url_for('search'))
    else:
        # Display update form
        return render_template('update_form.html', record=record)


@app.route('/delete/<string:id>', methods=['GET', 'POST', 'DELETE'])
def newdelete(id):
    # Delete record from Elasticsearch by ID
    try:
        es.delete(index='users', id=id)

        # Redirect to search page
        return redirect(url_for('search'))
    except:
        return render_template('error.html')



@app.route('/currentoncall', methods=['GET', 'POST'])
def currentoncall():
    if request.method == 'POST':
        team_name = request.form.get('Team_Name')
        current_date = datetime.now()
        start_date = current_date.date()
        end_date = current_date.date()

        # Search Elasticsearch for records matching the criteria
        try:
            res = es.search(index='users', body={
                'query': {
                    'bool': {
                        'must': [
                            {'match': {'Team_Name': team_name}},
                            {'range': {'Start_Date': {'lte': start_date}}},
                            {'range': {'End_Date': {'gte': end_date}}}
                        ]
                    }
                }
            })
            hits = res['hits']['hits']
            if len(hits) == 0:
                # No records found
                return 'No results found'
            return render_template('currentoncall.html', hits=hits)
        except:
            return render_template('error.html')
    else:
        return render_template('currentoncall.html')


# Run the Flask app
if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True,port=5000)
