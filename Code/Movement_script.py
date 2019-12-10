import pymysql, os, json,time, datetime
from pathlib import Path


# read JSON files which is in the parent folder for filename in 
# os.listdir(/home/rajaneesh/DARPG/griviences_Movement) parses the
# code and inserts the same into the Movement table in mysql database

# connect to MySQL database CPGRAM
con = pymysql.connect(host = 'localhost',user = 'rajan',passwd = '<password>',db = 'CPGRAM')
cursor = con.cursor()
cursor.execute("TRUNCATE TABLE Movement")

pathlist = Path('/home/rajaneesh/DARPG/griviences_Movement').glob('**/*.json')
for path in pathlist:
    print(path)


    # Iteratively read json file. Ignore all data except the records
    json_data=open(path).read()
    json_obj1 = json.loads(json_data)
    json_obj = json_obj1['records']


    # do validation and checks before insert
    def validate_string(val):
        if val != None:
            if type(val) is int:
                #for x in val:
                #   print(x)
                return str(val).encode('utf-8')
            else:
                return val




    # parse json data to SQL insert
    for i, item in enumerate(json_obj):
        registration_no=validate_string(item.get("registration_no",None))
        action_srno=validate_string(item.get("action_srno",None))
        action_name=validate_string(item.get("action_name",None))
        date_of_action=validate_string(item.get("date_of_action",None))
        org_name=validate_string(item.get("org_name",None))
        org_name2=validate_string(item.get("org_name2",None))
        remarks=validate_string(item.get("remarks",None))


        cursor.execute("INSERT INTO Movement(registration_no, action_srno, action_name, date_of_action, org_name, org_name2, remarks) VALUES (%s,%s,%s,%s,%s,%s,%s)", (registration_no, action_srno, action_name, date_of_action, org_name, org_name2, remarks))
    con.commit()
    

con.close()

