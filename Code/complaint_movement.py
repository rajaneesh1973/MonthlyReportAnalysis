# Import the necessary libraries

import requests
import time
import math

# Total record count in griviences movement database is 19,36,128. Download all the datas
# to store in the local database as json file. Each of the json file contains 10,000 records
total_datasize=1936128
request_size=10000
i=0

# iteration size
size=math.ceil(total_datasize/request_size)
for file_count in range(1,size):
    try:
	# specify the json filebame and open it in write mode
        filename=str(file_count)+".json"
        cpgrams_file = open(filename,"w+")

        # Get the url for data. replace yourkey with generated key value
        URL= 'https://api.data.gov.in/restricted/movement_cpgrams?api-key=<yourkey>&format=json&offset='+str(i*request_size)+'&limit=10000'
        print(URL)
        print(filename)

        # send the request to retrieve the data and capture the reponse and put the text in the file
        response =requests.get(URL)
        cpgrams_file.write(response.text) 
        #r ='https://api.data.gov.in/restricted/movement_cpgrams?api-key=<yourkey>&format=json&offset=310000&limit=10000'
        #cpgrams_file.write(r)
        
        # close the file and sleep for 10 second before heading to the last iteration
        cpgrams_file.close()
        time.sleep(10) 
    except:
        print("An exception occurred") 
        try:
            # if there is any timeout error try the same request of processing for two more times
            filename=str(file_count)+".json"
            cpgrams_file = open(filename,"w+")
            URL= 'https://api.data.gov.in/restricted/movement_cpgrams?api-key=<yourkey>&format=json&offset='+str(i*request_size)+'&limit=10000'
            print(URL)
            print(filename)
            response =requests.get(URL)
            cpgrams_file.write(response.text) 
            #r ='https://api.data.gov.in/restricted/movement_cpgrams?api-key=<yourkey>&format=json&offset=0&limit=10000'
            #cpgrams_file.write(r)
            cpgrams_file.close()
            time.sleep(10) 
        except:
            print("An exception occurred second time") 
            filename=str(file_count)+".json"
            cpgrams_file = open(filename,"w+")
            URL= 'https://api.data.gov.in/restricted/movement_cpgrams?api-key=<yourkey>&format=json&offset='+str(i*request_size)+'&limit=10000'
            print(URL)
            print(filename)
            response =requests.get(URL)
            cpgrams_file.write(response.text) 
            #r ='https://api.data.gov.in/restricted/movement_cpgrams?api-key=<yourkey>&format=json&offset=0&limit=10000'
            #cpgrams_file.write(r)
            cpgrams_file.close()
            time.sleep(10) 
    i=i+1
# Logic to save the last residual set of records where the record count will be less than 10,000


filename=str(i+1)+".json"
cpgrams_file = open(filename,"w+")

last_data_count=total_datasize%request_size  
URL='https://api.data.gov.in/restricted/movement_cpgrams?api-key=579b464db66ec23bdd00000120f78a60d0f24c3453640cfb17616bfb&format=json&offset='+str(i*request_size)+'&limit='+str(last_data_count)
print(URL)
print(filename)
response =requests.get(URL)
cpgrams_file.write(response.text) 
#r ='https://api.data.gov.in/restricted/movement_cpgrams?api-key=<yourkey>&format=json&offset=0&limit=10000'
#cpgrams_file.write(r)
cpgrams_file.close()        


