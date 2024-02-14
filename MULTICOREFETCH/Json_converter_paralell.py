import json
import jsonlines
from io import BytesIO
from concurrent import futures
import copy,os
from pprint import pprint


#For sorting files in the folder
def last_chars(x):
    return int(((os.path.basename(x)).replace("Multi","*").replace(".txt",'*').split("*"))[1])

def worker_function(files,initial):

     list=[]
     index = initial
     print("batch started:  "+str(initial) )
    #  print(files)
    #reading file from files batch
     for fil in files:
        print(fil)
        with open(fil, 'r') as f:
            lines=f.readlines()   
            f.close()

        linenumst=[]
        linenumcl=[]
         #getting linestart and linend of Warc data
        for x in range(len(lines)):
            if(lines[x].startswith('WARC-Target-URI')):
                if(len(linenumst)!=len(linenumcl)):
                    linenumcl.append(x-2)
                linenumst.append(x-2)
        if len(linenumst)>len(linenumcl):
            linenumcl.append(len(lines))
        dict1={}

     

# Paralell processing code
        for x in range(len(linenumst)):
            index += 1
            #Getting value of id,url,title and text which are required for us to compute
            URIindex=linenumst[x]+2
            # dict1['id']=(((lines[URIindex+2]).split('uuid:'))[1])[:-2]
            dict1["id"] = str(index)
            dict1['url']= ((lines[URIindex].split('URI:'))[1])[:-1]
            dict1['title']= lines[URIindex+9]
            dict1['text']=" ".join(lines[URIindex+9:linenumcl[x]])
      # print("Printing dict")
      # print(dict1)
            list.append(copy.deepcopy(dict1))#to get all the data in json format

     with jsonlines.open("Batch--"+str(int(index/200))+"Dev.to"+".txt",mode="w") as writer:
        for x in list:
            writer.write(x)#Writing in json format




#Main function
if __name__ == '__main__':
    chunk_size=10
    path =r'/Users/csuftitan/Downloads/futures' #Mention the folder path which would have downloaded and filtered wet files
    list_of_files=[]

    #Getting files from path
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".txt"):
                list_of_files.append(os.path.join(root,file))
    list_of_files.sort(key=last_chars)

    total=len(list_of_files)
    # print(total)
    # print(list_of_files)
    batch_end=0

    #Multi processing
    with futures.ProcessPoolExecutor(max_workers=5) as executor :
       #Calling worker function and sending batches of selected files  to cores to process

        for x in range(0,total,chunk_size):
            if(x+chunk_size<total):
                batch_end=x+chunk_size
            else:
                batch_end=total
                print(batch_end)
            print(batch_end)
            tasks = [executor.submit(worker_function,list_of_files[x:batch_end],(x*200))]
            batch_value=0
        for task in futures.as_completed(tasks):
            # retrieve the result
            if(batch_value+chunk_size<total):
                batch_value=batch_value+chunk_size
            else:
                batch_value=total
            # retrieve the result
                task.result()
            print("Progress is "+str((batch_value/total)*100))










# # # pprint(dict1)

# with open("/Users/csuftitan/Batch_wise_json/Test0.txt", 'r') as f:
#       lines=f.readlines()
#       print(lines[0])
