
from comcrawl import IndexClient
import requests,os,time
import gzip,shutil,zipfile,tarfile
from io import BytesIO

from pprint import pprint

word = 'medium.com' #change it to the desired domain
start_time=time.time()

a=-1 #initaite index

#Getting data from Comcrawl
client=IndexClient(["2022-40","2022-33"]) #Change the target month as needed
client.search("*."+str(word))
total=len(client.results)
pprint(str(total))

#iterate over the Comcrawl data
for x in client.results[a+1:(total)]:
 retry=15 # Using variable retry to reconnect when any error occured in sending requests to commoncrawl
 a+=1
 print("Progress-- "+str(a)+"/"+str(total)+" is "+str((a/total)*100))
 #Storing file info which do not have status 200 for furthey yse if required
 if(x['status']!='200'):
     if os.path.exists('Notstatus200.txt'): 
       append_write = 'a' # append if already exists
     else:
       append_write = 'w' # make a new file if not
     with open("Notstatus200.txt",append_write) as outp:
        outp.write("\n"+str(a)+" "+str(x['status'])+" "+str(x['filename']))
        #Writing data to file

# Should replace 4th part after split with wet and replace warc.gz with warc.wet.gz to get a valid link mentioned by Commoncrawl
 split=x['filename'].split("/")
 split[4]="wet" 
 Reformedlink=""

 for i in split:
    Reformedlink+="/"+i

 decompressed_file= None
 y="https://data.commoncrawl.org"+Reformedlink.replace('warc.gz','warc.wet.gz')#generating link as prescribed to download .gz file as format specified by commoncrawl 
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


 

#  Filtering wet for the domain specific data
 
 with open("/Users/csuftitan/Desktop/CommonCrawl/"+OUTFILE_NAME, 'r') as f:
  lines=f.readlines()
  f.close()
  linenumst=[] #to store starting lines from files which contain only medium.com related data
  linenumcl=[] #to store Finish lines from files which contain only medium.com related data
  
 #iterating over the lines in the file
 for x in range(len(lines)):

    if(lines[x].startswith('WARC-Target-URI')):
     if(len(linenumst)!=len(linenumcl)):
        linenumcl.append(x-2)
     words = lines[x].split() 
     for i in words:
        if(word in i): #Filtering using domain name
            linenumst.append(x-2)
 #To cover the case for last section of warc data
 if len(linenumst)>len(linenumcl):
   linenumcl.append(len(lines))
 if os.path.exists("CC-"+str(word)+"-Results.txt"):
    append_write = 'a' # append if already exists
 else:
    append_write = 'w' # make a new file if not

 with open("CC-"+str(word)+"-Results.txt",append_write) as out:
  out.write("\n"+"-----#"+"\n")
  for j in range(len(linenumst)):
    for k in range(linenumst[j],linenumcl[j]):
     out.write(lines[k]) #writing to file
  out.close()
  os.remove("/Users/csuftitan/Desktop/CommonCrawl/"+OUTFILE_NAME)
print("--------%s seconds------------" %(time.time()-start_time) )



 

