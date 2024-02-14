import json
from io import BytesIO
from concurrent import futures
import copy
from pprint import pprint


def worker_function(lines, initial,linenumst,linenumcl):
     dict1={}
     list=[]
     print(linenumst)
     id_val=initial

# Paralell processing code
     for x in range(len(linenumst)):
     #Getting value of id,url,title and text which are required for us to compute
      URIindex=linenumst[x]+2
      id_val=id_val+1
      #dict1['id']=(((lines[URIindex+2]).split('uuid:'))[1])[:-2] 
      dict1['id']=str(id_val)
      dict1['url']= ((lines[URIindex].split('URI:'))[1])[:-1]
      dict1['title']= lines[URIindex+9]
      dict1['text']=" ".join(lines[URIindex+9:linenumcl[x]])
      # print("Printing dict")
      # print(dict1)
      list.append(copy.deepcopy(dict1))#to get all the data in json format
    
     out_file = open("Test"+str(initial)+".json", "w")
     json.dump(list, out_file, indent = 4)#Writing in json format
     out_file.close()


#Main function
if __name__ == '__main__':
     chunk_size=100 #defining chunk size which would be diviide between given cores
     
     filename="/Users/csuftitan/Desktop/CC-Medium-Results-Multi23471.txt" #pass file name
    
    #reading file and getting linestart and linend of Warc data
     with open(filename, 'r') as f:
      lines=f.readlines()
      linenumst=[]
      linenumcl=[]
     for x in range(len(lines)):
      if(lines[x].startswith('WARC-Target-URI')):
       if(len(linenumst)!=len(linenumcl)):
        linenumcl.append(x-2)
       linenumst.append(x-2)
     if len(linenumst)>len(linenumcl):
      linenumcl.append(len(lines))

     total=len(linenumst)
     print(total)
     batch_value=0 #initial value

 


     
     #Multi processing 
     with futures.ProcessPoolExecutor(max_workers=5) as executor :
      #Calling worker function and sending chunks selected lines  to cores to process
      tasks = [executor.submit(worker_function,lines,batch_value,linenumst[x:x+chunk_size],linenumcl[x:x+chunk_size]) for x in range(0,total,chunk_size)]
      for task in futures.as_completed(tasks):
           
            if(batch_value+chunk_size<total): 
             batch_value=batch_value+chunk_size
            else:
              batch_value=total            
             # retrieve the result
            task.result()
            print("Progress is "+str((batch_value/total)*100))








