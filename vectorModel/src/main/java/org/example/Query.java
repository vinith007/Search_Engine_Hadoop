package org.example;

import java.util.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;


public class Query {

    public static class QueryMapper
            extends Mapper<Object, Text, FloatWritable, Text>{


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            // get configuration
            Configuration conf = context.getConfiguration();

            String line = value.toString(); // acquiring the doc
            // if line contains info about any doc
            if (line.contains("length")) {
                // obtain doc id
                String id = line.substring(0, line.indexOf("length")).replaceAll(" ", "");
                // obtain doc tf/idf representation
                String tfidf_t = line.substring(line.indexOf("{") + 1, line.indexOf("}"));
                line = null;    //free memory
                String cur_word;
                String cur_val;
                Float r = (float) 0;

                String[] tfidfs = tfidf_t.split(",");
                tfidf_t = null;
                // iterating through all words present in doc
                for (String t : tfidfs) {
                    if (t.contains("=")) {
                        cur_word = t.substring(0, t.indexOf("=")).replaceAll(" ", "");  // obtaining word
                        cur_val = t.substring(t.indexOf("=") + 1).replaceAll(" ", "");  // obtaining  word tf/idf score for doc
                        // calculate relevance score
                        r += Float.parseFloat(conf.get("!!query!!" + cur_word, "0")) * Float.valueOf(cur_val);
                    }

                }

                context.write(new FloatWritable(r), new Text(id));
            }
        }
    }

    public static class QueryComparator
            extends WritableComparator {

        protected QueryComparator() {
            super(FloatWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            // custom comparator to sort keys (relevance score) in a descending order instead of ascending
            FloatWritable f1 = (FloatWritable)a;
            FloatWritable f2 = (FloatWritable)b;
            return f2.compareTo(f1);
        }
    }

    public static class QueryReducer
            extends Reducer<FloatWritable, Text, FloatWritable, Text> {
        private IntWritable result = new IntWritable();

        public void reduce(FloatWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            // write sorted relevance score - doc id
            for (Text v: values) {
                context.write(key, v);
            }
        }
    }

    public static HashMap<Integer, String> getTitle(String folder, Set<Integer> ids, FileSystem fs) {
        HashMap<Integer, String> result = new HashMap<Integer, String>();
        HashSet<Integer> used = new HashSet<Integer>();
        BufferedReader reader;
        Path path;
        try {


            FileStatus[] fileStatuses = fs.listStatus(new Path(folder));


            for(FileStatus fStatus: fileStatuses) {
                String filename = fStatus.getPath().toString();


                path = new Path(folder + "/" + filename.substring(filename.indexOf(folder + "/") + (folder + "/").length()));

                reader = new BufferedReader(new InputStreamReader(fs.open(path)));

                String text_val = reader.readLine();
                while(text_val != null && used.size() != ids.size()){
                    Integer tempID = Integer.parseInt(text_val.substring(8, text_val.indexOf("\", \"url\"")).replaceAll(" ", "")); // getting doc id
                    if(ids.contains(tempID)) {
                        String title = text_val.substring(text_val.indexOf("title") + "title".length() + 4, text_val.indexOf("\", \"text"));
                        String url = text_val.substring(text_val.indexOf("url") + "url".length() + 4, text_val.indexOf("\", \"title"));

                        result.put(tempID, "Title: " + title + "    URL: " + url);
                        used.add(tempID);
                    }
                    text_val = reader.readLine();
                }

            }
        } catch (IOException e){
            e.printStackTrace();
        }
        return result;
    }

    

    public static void main(String[] args) throws Exception {

        


        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);


        if (args.length != 5) {
            System.out.println("program expects 5 arguments");
            
            
            System.exit(1);
        }

        Path input_path = new Path(args[3]);
        Path output_path = new Path(args[4]);
        String wiki = args[2];

        if (wiki.charAt(wiki.length() - 1) == '/') {
            wiki = wiki.substring(0, wiki.length()-1);
        }

        if (!fs.exists(input_path)) {
            System.out.println("unable to located input directory");
           
            System.exit(1);
        }

        if (fs.exists(output_path)) {
            System.out.println("output should not be created");
            
            System.exit(1);
        }


        Integer N = Integer.parseInt(args[1]);
        if (N < 0 || N > 1000) {
            System.out.println("range permintted is 10");
           
            System.exit(1);
        }


       
        BufferedReader reader;
        Path path;
        HashMap<String, Integer> idfs = new HashMap<String, Integer>();
        try {

            
            FileStatus[] fileStatuses = fs.listStatus(new Path("output_idf"));

            
            for(FileStatus fStatus: fileStatuses) {
                String filename = fStatus.getPath().toString();
                if (!filename.contains("SUCCESS")) {
                    
                    path = new Path("output_idf/" + filename.substring(filename.indexOf("output_idf/") + "output_idf/".length()));

                    reader = new BufferedReader(new InputStreamReader(fs.open(path)));

                    String line = reader.readLine();
                    while(line != null){
                        StringTokenizer itr = new StringTokenizer(line);
                        String cur_word = "";
                        Integer cur_idf = 0;

                        if(itr.hasMoreTokens()){
                            cur_word = itr.nextToken();
                            if (itr.hasMoreTokens()) {
                                cur_idf = Integer.parseInt(itr.nextToken().replaceAll("[^0-9]", ""));
                                idfs.put(cur_word, cur_idf);
                            }
                        }

                        line = reader.readLine();
                    }
                }
            }
        } catch (IOException e){
            e.printStackTrace();
        }

        String query = args[0];
        StringTokenizer itr = new StringTokenizer(query.toLowerCase().replaceAll("\\\\[a-z]", " ").replaceAll("-", " "));
        String word = "";
        Integer sum = 0;
        Integer cur_idf = 0;
        Float r = new Float(0);
        HashMap<String, Integer> queryMap = new HashMap<String, Integer>(); // for storing word -> tf


        while(itr.hasMoreTokens()) {

            word = itr.nextToken().replaceAll("[\\\\0-9~`!@#$%^&*()\\-_+=\\,.<>?/'\":;{}\\[\\]\\|]", "");

            if (!queryMap.containsKey(word)) {
                queryMap.put(word, 1);
            }
            else {
                queryMap.put(word, queryMap.get(word) + 1);
            }
        }

        // iterating through all words to create tf-idf score for each
        Set<String> keySet = queryMap.keySet();
        for (String k: keySet) {
            // obtaining idf for word
            cur_idf = idfs.get(k);
            // divide tf by idf
            if (cur_idf != null) {
                r = (float) queryMap.get(k) / (float) cur_idf;

                // write query tf/idf for word for mapreduce
                conf.set("!!query!!" + String.valueOf(k.hashCode()), Float.toString(r));
            }
        }
        ///////////// QUERY TO TF-IDF /////////////



        // mapreduce job
        Job job = Job.getInstance(conf, "query");
        job.setJarByClass(Query.class);
        job.setMapperClass(QueryMapper.class);
        job.setCombinerClass(QueryReducer.class);
        job.setSortComparatorClass(QueryComparator.class);
        job.setReducerClass(QueryReducer.class);
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(args[3]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        job.waitForCompletion(true);



        Integer totalCount = 0;
        Float tempValue = (float) 0;
        Integer tempID = 0;

        HashMap<Integer, Float> top = new HashMap<Integer, Float>();

        ArrayList<Integer> arr = new ArrayList<Integer>();

        try {

            
            FileStatus[] fileStatuses = fs.listStatus(new Path(args[4]));

            
            for(FileStatus fStatus: fileStatuses) {
                if (totalCount >= N){
                    break;
                }

                String filename = fStatus.getPath().toString();
                if (!filename.contains("SUCCESS")) {
                   
                    path = new Path(args[4] + "/" + filename.substring(filename.indexOf(args[4]) + args[4].length() + 1));

                    reader = new BufferedReader(new InputStreamReader(fs.open(path)));

                    String line = reader.readLine();
                    while(line != null && totalCount < Integer.parseInt(args[1])){
                        itr = new StringTokenizer(line);
                        
                        if(itr.hasMoreTokens()){
                           
                            tempValue = Float.parseFloat(itr.nextToken());
                            if (itr.hasMoreTokens()) {
                                
                                tempID = Integer.parseInt(itr.nextToken().replaceAll("[^0-9]", ""));
                             
                                top.put(new Integer(tempID), new Float(tempValue));
                                arr.add(tempID);
                            }
                        }

                        totalCount++;
                        line = reader.readLine(); // reading the next line
                    }
                }
            }
        } catch (IOException e){
            e.printStackTrace();
        }

        
        HashMap<Integer, String> title = getTitle(wiki, top.keySet(), fs);

      
        for(Integer i: arr){
            System.out.println("Id: " + i.toString() + "    " + title.get(i) + "   Score: " + top.get(i).toString());
        }

        System.exit(1);
        

    }
}
