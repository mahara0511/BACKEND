import lancedb
import os
import hashlib
import uuid
import pandas as pd
import bcrypt
import datetime
import requests
import pyarrow as pa
from flask import (Flask, redirect, render_template, request,
                   send_from_directory, session, url_for)
from flask_session import Session
from PIL import Image
from flask import Flask, jsonify, request , Response

app = Flask(__name__)

lancedb_instance = lancedb.connect('database.lance')
TABLE_NAME = ['USER','PRINTER','LOCATION','FILE','PRINTING JOB','CONFIGURATION','REPORT','EVENT','REPORT EVENT']



if 'USER' not in lancedb_instance.table_names():
    user_schema = pa.schema([
        pa.field('id' , pa.string()),
        pa.field('name',pa.string()),
        pa.field('email',pa.string()),
        pa.field('password',pa.string()),
        pa.field('avatar_url',pa.string()),
        pa.field('role',pa.string()),
        pa.field('balance',pa.int32()),
        pa.field('pages',pa.int32())
    ])

    lancedb_instance.create_table('USER',schema=user_schema)

users = lancedb_instance['USER']


if 'LOCATION' not in lancedb_instance.table_names():
    location_schema = pa.schema([
        pa.field('id',pa.int32()),
        pa.field('building',pa.string()),
        pa.field('room',pa.string())
    ])

    lancedb_instance.create_table('LOCATION',schema=location_schema)

locations = lancedb_instance['LOCATION']
if 'PRINTER' not in lancedb_instance.table_names():
    printer_schema = pa.schema([
        pa.field('id',pa.string()),
        pa.field('brand',pa.string()),
        pa.field('model',pa.string()),
        pa.field('description',pa.string()),
        pa.field('status',pa.string()),
        pa.field('location_id',pa.int32())
    ])

    lancedb_instance.create_table('PRINTER',schema=printer_schema)

printers = lancedb_instance['PRINTER']



if 'FILE' not in lancedb_instance.table_names():
    file_schema = pa.schema([
        pa.field('id',pa.string()),
        pa.field('userID',pa.string()),
        pa.field('fileName',pa.string()),
        pa.field('isDeleted',pa.bool_()),
        pa.field('url',pa.string()),
        pa.field('type',pa.string()),
        pa.field('time',pa.string()),
        pa.field('size',pa.int64())#size in bytes
    ])

    lancedb_instance.create_table('FILE',schema=file_schema)

files = lancedb_instance['FILE']


if 'ORDER' not in lancedb_instance.table_names():
    order_schema = pa.schema([
        pa.field('id',pa.string()),
        pa.field('printerID',pa.string()),
        pa.field('userID',pa.string()),
        pa.field('fileID',pa.string()),
        pa.field('time', pa.string()),
        pa.field('paperSize',pa.string()),
        pa.field('numPage',pa.int32()),
        pa.field('numSide', pa.int32()),
        pa.field('numCopy',pa.int32()),
        pa.field('scale',pa.string()),
        pa.field('orientation',pa.string()),
        pa.field('status',pa.string())
    ])
    lancedb_instance.create_table('ORDER',schema=order_schema)

orders = lancedb_instance['ORDER']


if 'CONFIG' not in lancedb_instance.table_names():
    config_schema = pa.schema([
        pa.field('id',pa.string()),
        pa.field('userID',pa.string()),
        pa.field('numPage',pa.int32()),
        pa.field('dateGivePage',pa.string()),
    ])
    lancedb_instance.create_table('CONFIG',schema=config_schema)

configs = lancedb_instance['CONFIG']


if 'BUY_PAGE' not in lancedb_instance.table_names():
    buy_page_schema = pa.schema([
        pa.field('id',pa.int32()),
        pa.field('userID',pa.string()),
        pa.field('time',pa.string()),
        pa.field('amount',pa.int32()),
        pa.field('method',pa.string()),
        pa.field('status',pa.string())
    ])
    lancedb_instance.create_table('BUY_PAGE',schema=buy_page_schema)

buy_pages = lancedb_instance['BUY_PAGE']


if 'REPORT' not in lancedb_instance.table_names():
    report_schema = pa.schema([
        pa.field('id',pa.string()),
        pa.field('name',pa.string()),
        pa.field('created_time',pa.string()),
        pa.field('type',pa.string()),
        pa.field('description', pa.string())
    ])
    lancedb_instance.create_table('REPORT',schema=report_schema)

reports = lancedb_instance['REPORT']



if 'EVENT' not in lancedb_instance.table_names():
    event_schema = pa.schema([
        pa.field('id',pa.string()),
        pa.field('userID',pa.string()),
        pa.field('created_time',pa.string()),
        pa.field('type',pa.string()),
        pa.field('description', pa.string())
    ])
    lancedb_instance.create_table('EVENT',schema=event_schema)

events = lancedb_instance['EVENT']


if 'REPORT_EVENT' not in lancedb_instance.table_names():
    report_event_schema = pa.schema([
        pa.field('id',pa.string()),
        pa.field('reportID',pa.string()),
        pa.field('eventID',pa.string())
    ])
    lancedb_instance.create_table('REPORT_EVENT',schema=report_event_schema)

report_events = lancedb_instance['REPORT_EVENT']

#utils function
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()


# A simple endpoint to return a greeting
@app.route('/')
def hello_world():
    return render_template("index.html")

#register
@app.route('/v1/api/register' , methods=['POST'])
def register():
    global users
    username = request.form['username']
    email = request.form['email']
    id = str(1)
    password = bcrypt.hashpw(request.form['password'].encode(),bcrypt.gensalt()).decode()
    if 'avatar_url' in request.form:
        print('There is avatar url')

    # Create a new user record

    df = pd.DataFrame(columns=['id','name','email','password','avatar_url','role','balance','pages'])

    new_user = {
        "id": [id],
        "name": [username],
        "email": [email],
        "password": [password],
        "avatar_url" : [''],
        'role' : ['user'],
        'balance' : [0],
        'pages' : [40]
    }
    df = pd.concat([df,pd.DataFrame(new_user)],ignore_index=True)
    users.add(df)
    # Write the new user to the LanceDB dataset
    new_user_response = {
        "id": id,
        "name": username,
        "email": email,
        "password": password,
        "avatar_url" : '',
        'role' : 'user',
        'balance' : 0,
        'pages' : 40
    }
    response = {
        'status' : 200,
        'message' : 'register successfully',
        'data' : new_user_response
    }

    return jsonify(response)


@app.route('/v1/api/login',methods=['POST'])
def login():
    global users
    table = users.to_pandas()
    email = request.form['email']
    password = request.form['password']
    user = table[table['email'] == email]
    if len(user) == 0 :
        return Response('Unauthorized',mimetype='text/plain',status = 401)
    else:
        hash_password = user.at[0,'password']
        if bcrypt.checkpw(password.encode(),hash_password.encode()):
            response = {
                'status' : 200 ,
                'message' : 'login successfully',
                'data' : user.iloc[0].to_dict()
            }
            return jsonify(response)
        else:
            return Response('Unauthorized',mimetype='text/plain',status = 401)
        

@app.route('/v1/api/uploadFile',methods = ['POST'])
def upload():
    FILE_PATH = 'file_uploading/'
    global files
    userID = "1"
    if request.method == 'POST' :
        if 'file' not in request.files or request.files['file'].filename == '':
            response = {
                "status" : 400,
                'message' : "No file to save"
            }
            return Response('No file to save',status=400)
        
        else:
            f = request.files['file']
            path = FILE_PATH + f.filename
            f.save(path)
            id = str(uuid.uuid4())
            created_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df = pd.DataFrame(columns=['id',"userID","filename","isDeleted","url",'type','time','size'])
            new_file = {
                "id" : [id],
                "userID" : [userID],
                "fileName" : [f.filename],
                "isDeleted" : [False],
                "url" : [path],
                'type' : [f.content_type],
                'time' : [created_time],
                'size' : [os.path.getsize(path)]
            }

            df = pd.concat([df,pd.DataFrame(new_file)],ignore_index=True)
            files.add(df)

            data = {
                "id" : id,
                "userID" : userID,
                "fileName" : f.filename,
                "isDeleted" : False,
                "url" : path,
                'type' : f.content_type,
                'time' : created_time,
                'size' : os.path.getsize(path)
            }
            response = {
                'status' : 200,
                'message' : 'upload successfully',
                'data' : data
            }
            return jsonify(response)


@app.route('/v1/api/file/user/<string:user_ID>' , methods = ['GET'])
def get_file_by_user(user_ID):
    global files
    table = files.to_pandas()
    user_files = table[table['userID']==user_ID]
    data = []
    for i , row in user_files.iterrows():
        data.append(row.to_dict())


    response = {
        'status' : 200,
        'message' : 'Get file successfully',
        'data' : data
    }
    return jsonify(response)



@app.route('/v1/api/file/<string:fileID>' , methods = ['GET'])
def get_file_by_fileID(fileID):
    global files
    table = files.to_pandas()
    user_files = table[table['id']==fileID]
    data = []
    for i , row in user_files.iterrows():
        data.append(row.to_dict())

    response = {
        'status' : 200,
        'message' : 'Get file successsfully',
        'data' : data
    }
        

    return jsonify(response)



@app.route('/v1/api/file' , methods = ['GET'])
def get_all_files():
    global files
    table = files.to_pandas()
    data =  []
    for i , row in table.iterrows():
        data.append(row.to_dict())

    response = {
        'status' : 200,
        'message' : 'Get file successsfully',
        'data' : data
    }
        

    return jsonify(response)


@app.route('/v1/api/file/delete' , methods = ['DELETE'])
def delete_file():
    global files
    fileID = request.form['fileID']
    table = files.to_pandas()

    if len(table[table['id']==fileID]) == 0:
        response = {
            'status' : 404 ,
            'message' : 'No such file to delete'
        } 
        return jsonify(response)

    if (table[table['id']==fileID].iloc[0].to_dict()['isDeleted'] == True):
        response = {
            'status' : 404 ,
            'message' : 'File already deleted'
        }


        return jsonify(response)

    os.remove(table[table['id']==fileID].iloc[0].to_dict()['url'])
    files.update(where='id = ' + '"' + fileID + '"' , values={'isDeleted' : True , 'url' : ''})

    table = files.to_pandas()
    print(table)


    response = {
        'status' : 200,
        'message' : 'Deleted successfully',
        'data' : table[table['id']==fileID].iloc[0].to_dict()
    }


    return jsonify(response)


@app.route('/v1/api/user/<string:userID>' , methods = ['GET'])
def get_user_profile(userID):
    global users
    table = users.to_pandas()
    user_table = table[table['id'] == userID]
    if len(user_table) == 0 :
        return Response('No user found' , status=404)
    
    else:
        data = user_table.iloc[0].to_dict()
        response = {
            'status' : 200 , 
            'message' : 'Found user ',
            'data' : data
        }

        return jsonify(response)
    

@app.route('/v1/api/order/create' , methods = ['POST'])
def create_print_order():
    global orders
    global files
    FILE_PATH = 'file_uploading/'
    userID = "1"
    if request.method == 'POST' :
        if 'file' not in request.files or request.files['file'].filename == '':
            response = {
                "status" : 400,
                'message' : "No file to save"
            }
            return Response('No file to save',status=400)
        
        else:
            f = request.files['file']
            path = FILE_PATH + f.filename
            f.save(path)
            id = str(uuid.uuid4())
            df = pd.DataFrame(columns=['id',"userID","filename","isDeleted","url",'type','time','size'])
            new_file = {
                "id" : [id],
                "userID" : [userID],
                "fileName" : [f.filename],
                "isDeleted" : [False],
                "url" : [path],
                'type' : [f.content_type],
                'time' : [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                'size' : [os.path.getsize(path)]
            }

            df = pd.concat([df,pd.DataFrame(new_file)],ignore_index=True)
            files.add(df)
    fileID = id
    numcopy = request.form['numCopy']
    numpage = request.form['numPage']
    orientation = request.form['orientation']
    scale = request.form['scale']
    papersize = request.form['paperSize']   
    numside = request.form['numSide']
    printerID = request.form['printerID']
    id = len(orders.to_pandas()) + 1


    userID = "1"




    
    df = pd.DataFrame(columns=['id','printerID','userID','fileID','time','paperSize','numPage','numSide','numCopy','scale','orientation','status'])


    new_order_row = {
        'id' : [id],
        'printerID' : [printerID],
        'userID' : [userID],
        'fileID' : [fileID],
        'time' : [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
        'paperSize' : [papersize],
        'numPage' : [int(numpage)],
        'numSide' :[int(numside)],
        'numCopy' : [int(numcopy)],
        'scale' : [scale],
        'orientation' : [orientation],
        'status' : ['Progressing']
    }
    new_order_row = pd.DataFrame(new_order_row)
    df = pd.concat([df,new_order_row] , ignore_index=True)
    orders.add(df)

    response = {
        'status' : 200 ,
        'message' : 'Send order request successfully',
        'data' : new_order_row.iloc[0].to_dict()
    }

    return jsonify(response)


@app.route('/v1/api/buyPages' , methods = ['POST'])
def user_buy_pages():
    global buy_pages
    global users
    userID = "1"


    numPage = request.form['numPage']
    cost = request.form['cost']
    method = request.form['method']
    id = len(buy_pages.to_pandas()) + 1 



    tbl = users.to_pandas()
    user = tbl[tbl['id'] == userID]

    if len(user) == 0 :
        return Response('No user found' , status=400)
    
    else:
        status = None
        user = user.iloc[0].to_dict()
        if user['balance'] < int(cost):
            status = 'Failed'
        
        else:
            users.update(where='id = "1"' , values={'balance' : user['balance'] - int(cost) , 'pages' : user['pages'] + int(numPage)})
            status = 'Successful'
        df = pd.DataFrame(columns=['id' , 'userID' , 'time' , 'amount' , 'method' , 'status'])
        new_record = {
            'id' : [id] ,
            'userID' : [userID],
            'time' : [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
            'amount' : [numPage],
            'method' : [method] , 
            'status' : [status]
        }

        new_record = pd.DataFrame(new_record)

        df = pd.concat([df,new_record] , ignore_index=True)

        buy_pages.add(df)
        if status == 'Failed':
            response = {
                'status' : 404 , 
                'message' : 'Buy pages failed',
                'data' : new_record.iloc[0].to_dict()
            }
        else:
            response = {
                'status' : 200 , 
                'message' : 'Buy pages successfully',
                'data' : new_record.iloc[0].to_dict()
            }


        return jsonify(response)
    
@app.route('/v1/api/buyPages/records', methods=['GET'])
def show_all_records():
    global buy_pages

    try:
        # Lấy toàn bộ dữ liệu từ bảng buy_pages
        records = buy_pages.to_pandas()
        
        # Chuyển dữ liệu thành dạng dictionary
        data = records.to_dict(orient='records')

        response = {
            'status': 200,
            'message': 'Records fetched successfully',
            'data': data
        }
        return jsonify(response)

    except Exception as e:
        response = {
            'status': 500,
            'message': 'An error occurred while fetching records',
            'error': str(e)
        }
        return jsonify(response), 500


@app.route('/v1/api/getHistory/<string:userID>' , methods = ['GET'])
def get_history(userID):
    global orders
    tbl = orders.to_pandas()
    history = tbl[tbl['userID'] == userID]
    data = []
    for i , row in history.iterrows():
        data.append(row.to_dict())

    response = {
        'status' : 200 ,
        'message' : 'Get history successfully',
        'data' : data
    }

    return jsonify(response)


@app.route('/v1/api/updateStatus/<string:orderID>' , methods = ['POST'])
def update_order_status(orderID):
    global orders
    status = request.form['status']
    orders.update(where=f"id = " + '"' + orderID + '"' , values={'status' : status})

    tbl = orders.to_pandas()
    data = tbl[tbl['id'] == orderID].iloc[0].to_dict()

    response = {
        'status' : 200 ,
        'message' : 'update order status successfully',
        'data' : data
    }

    return jsonify(response)
    


if __name__ == '__main__':
    app.run(host='localhost' , port=8080 , debug=True)
