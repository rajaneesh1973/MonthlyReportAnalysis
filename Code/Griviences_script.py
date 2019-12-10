import pymysql, os, json,time, datetime
from pathlib import Path


# read JSON files which is in the parent folder for filename in 
# os.listdir(/home/rajaneesh/DARPG/griviences) parses the code and 
# inserts the same into the Complaint table in mysql database

# connect to MySQL
con = pymysql.connect(host = 'localhost',user = 'rajan',passwd = '<password>',db = 'CPGRAM')
cursor = con.cursor()
cursor.execute("TRUNCATE TABLE Complaint")

#pathlist = Path('/home/rajaneesh/DARPG/griviences').glob('**/*.json')
pathlist=["1.json", "2.json", "3.json", "4.json", "5.json", "6.json", "7.json", "8.json", "9.json", "10.json", "11.json", "12.json", "13.json", "14.json", "15.json", "16.json", "17.json", "18.json", "19.json", "20.json", "21.json", "22.json", "23.json", "24.json", "25.json", "26.json", "27.json", "28.json", "29.json", "30.json", "31.json"]

for i in range(len(pathlist)):

   

    path = os.path.abspath('/home/rajaneesh/DARPG/griviences') +"/"+ pathlist[i]
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
        ministry_department=validate_string(item.get("ministry_department",None))
        country_name=validate_string(item.get("country_name",None))
        state_name=validate_string(item.get("state_name",None))
        distname=validate_string(item.get("distname",None))
        subject_content=validate_string(item.get("subject_content",None))
        diarydate=validate_string(item.get("diarydate",None))
        #diarydate= time.strptime(diarydate, "%d-%m-%Y %H:%M")
        #diarydate = diarydate.strftime('%Y-%m-%d %H:%M:%S')
 
        #print(diarydate)
        closing_date=validate_string(item.get("closing_date",None))
        SourceName=validate_string(item.get("SourceName",None))
        rating=validate_string(item.get("rating",None))
        comments=validate_string(item.get("comments",None))
        ratingdate=validate_string(item.get("ratingdate",None))


        cursor.execute("INSERT INTO Complaint(registration_no, ministry_department, country_name, state_name, distname, subject_content, diarydate, closing_date, SourceName, rating, comments, ratingdate) VALUES ( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (registration_no,	ministry_department,	country_name, state_name, distname, subject_content, diarydate, closing_date, SourceName, rating, comments, ratingdate))
    con.commit()
    

con.close()

