import multiprocessing as mp
from comcrawl import IndexClient
import requests,os,time
import gzip,shutil,zipfile,tarfile
from io import BytesIO
from concurrent import futures
from pprint import pprint
from pprint import pprint

def worker_function(items, initial,total,word):
  a=initial #initaite index

  retry=15# Using variable retry to reconnect when any error occured in sending requests to commoncrawl
  
  #iterate over the batch
  for item in items:
    a=a+1
    print("Progress-- "+str(a)+"/"+str(total))

     #Storing file info which do not have status 200 for furthey yse if required
    if(item['status']!='200'):
    
     if os.path.exists('Notstatus200.txt'):
       append_write = 'a' # append if already exists
     else:
       append_write = 'w' # make a new file if not
 
     with open('Notstatus200.txt',append_write) as outp:
        outp.write("\n"+str(a)+" "+str(item['status'])+" "+str(item['filename'])) #Writing data to file

    # Should replace 4th part after split with wet and replace warc.gz with warc.wet.gz to get a valid link mentioned by Commoncrawl
    split=item['filename'].split("/")
    split[4]="wet"
    Reformedlink=""

    for i in split:
     Reformedlink+="/"+i
    
    y="https://data.commoncrawl.org"+Reformedlink.replace('warc.gz','warc.wet.gz') #generating link as prescribed to download .gz file as format specified by commoncrawl 
     
     #download and unzip the wet.gz file by sending requests to link
    CCfname = y.replace('wet.gz','*').replace('wet/','*').split("*")
    OUTFILE_NAME=CCfname[1]+".wet"


#sending reqests multiple times if failed
    for r in range(retry):
     data=requests.get(y,stream=True) #sending request
     compressed_file = BytesIO(data.content) 
     decompressed_file=gzip.GzipFile(fileobj=compressed_file) #Unzipping the gzip data
     try:
      with open("/Users/csuftitan/Desktop/CommonCrawl/"+OUTFILE_NAME,'wb') as outfile:
       outfile.write(decompressed_file.read()) #writing to file
       outfile.flush()
       outfile.close()
    
     except gzip.BadGzipFile:
      if r < retry - 1:
       print("tried "+str(r)+" times")
       time.sleep(3)
       continue
      else:
       print(y)
       raise Exception
     break
    
    with open("/Users/csuftitan/Desktop/CommonCrawl/"+OUTFILE_NAME, 'r') as f:
     lines=f.readlines()
     f.close()
     linenumst=[]#to store starting lines from files which contain only medium.com related data
     linenumcl=[] #to store Finish lines from files which contain only medium.com related data
  
  #iterating over the lines in the file
     for x in range(len(lines)):

      if(lines[x].startswith('WARC-Target-URI')):
        if(len(linenumst)!=len(linenumcl)):
         linenumcl.append(x-2)
        words = lines[x].split() 
        for i in words: #Filtering using domain name
          if(word in i):
            linenumst.append(x-2)

#To cover the case for last section of warc data
    if len(linenumst)>len(linenumcl):
     linenumcl.append(len(lines))
    
    
    if os.path.exists('CC-Medium-Results-Multi'+str(initial)+'.txt'):
     append_write = 'a' # append if already exists
    else:
     append_write = 'w' # make a new file if not
    
    with open('CC-Medium-Results-Multi'+str(initial)+'.txt',append_write) as out:
           

            out.write("\n")
            for j in range(len(linenumst)):
              for k in range(linenumst[j],linenumcl[j]):
                 out.write(lines[k])#writing to file
            out.flush()
            out.close()
            os.remove("/Users/csuftitan/Desktop/CommonCrawl/"+str(OUTFILE_NAME))
  
    






#Main function
if __name__ == '__main__':
   
    word = 'medium.com' #change it to the desired domain
    start_time=time.time()
  
    initial=-1 #initaite index
    

    #Getting data from Comcrawl
    client=IndexClient(["2022-40","2022-33"]) #Change the target month as needed
    client.search("*."+str(word))
    total=len(client.results)
    
    

    chunk_size=50 #defining chunk size which would be diviide between given cores

    #implementing multi core function with the same logic
    with futures.ProcessPoolExecutor(max_workers=5) as executor :
      #Calling the worker function
      tasks = [executor.submit(worker_function,client.results[x:x+chunk_size],x-1,total,word) for x in range(initial,total,chunk_size)]
      for task in futures.as_completed(tasks):
            # retrieve the result
            if(batch_value+chunk_size<total):
             batch_value=batch_value+chunk_size
            else:
              batch_value=total            
            task.result()
            print("Progress is "+str((batch_value/total)*100))

            
            

 
    print("--------%s seconds------------" %(time.time()-start_time) )