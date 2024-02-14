package org.example;
import java.io.*;
import org.apache.hadoop.fs.*;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
public class Indexer {

    public static class IDFVectorMap
            extends Mapper<Object, Text, Text, IntWritable>{

        
        private Text val_word = new Text();
        private final static IntWritable valOne = new IntWritable(1);

        public void map(Object key, Text doc, Context context
        ) throws IOException, InterruptedException {

            String line = doc.toString(); // reading a line containing json about doc
            String text = line.substring(line.indexOf(", \"text\":") + 9); // acquiring "text" part of json

            StringTokenizer itr = new StringTokenizer(text.toLowerCase()
                    .replaceAll("\\\\[a-z]", " ")
                    .replaceAll("-", " ")); //tokenizing text, removing symbols of tabulation and "-"

            line = null;
            text = null;

            String cur; // current token
            HashSet used = new HashSet(); // stores already processed tokens (words)

            // iterating through tokens
            while (itr.hasMoreTokens()) {
                // getting rid of non-letter symbols
                cur = itr.nextToken().replaceAll("[\\\\0-9~`!@#$%^&*()\\-_+=\\,.<>?/'\":;{}\\[\\]\\|]", "");
                if (! cur.equals("")) { // non-empty token
                    if (!used.contains(cur)) { // 1st occurrence
                        used.add(cur); // adding to processed tokens
                        val_word.set(cur);
                        context.write(val_word, valOne); // passing to reducer
                    }
                }
            }
        }
    }

    public static class IDFVectorReduce
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable output = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0; // number of documents in which the text word has appeared
            for (IntWritable val : values) {
                sum += val.get();
            }

            output.set(sum);
            context.write(key, output);
        }
    }

    public static class VecMap
            extends Mapper<Object, Text, Text, MapWritable>{

        private Text val_word;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString();
            String text = line.substring(line.indexOf(", \"text\":") + 9);
            String id = line.substring(8, line.indexOf("\", \"url\""));

            StringTokenizer itr = new StringTokenizer(text.toLowerCase().
                    replaceAll("\\\\[a-z]", " ")
                    .replaceAll("-", " "));

            String cur;


            text = null;
            line = null;

            MapWritable map = new MapWritable();


            while (itr.hasMoreTokens()) {

                cur = itr.nextToken().replaceAll("[\\\\0-9~`!@#$%^&*()\\-_+=\\,.<>?/'\":;{}\\[\\]\\|]", "");
                if (! cur.equals("")) { // excluding empty tokens
                    val_word = new Text(cur);
                    if (! map.containsKey(val_word)) {
                        map.put(val_word, new IntWritable(1));
                    }
                    else {
                        IntWritable r = (IntWritable) map.get(val_word);
                        map.put(val_word, new IntWritable(r.get() + 1));
                    }
                }
            }

            context.write(new Text(id), map);
        }
    }

    public static class VecReduce
            extends Reducer<Text, MapWritable, Text, MapWritable> {

        private static HashMap<String, Integer> get_idf = new HashMap<String, Integer>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            // fetching and reading from cache

            URI[] cacheFiles = context.getCacheFiles();

            for (URI cf: cacheFiles){
                FileSystem fs = FileSystem.get(conf);
                Path path = new Path(cf.toString());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));

                String line = reader.readLine();
                while(line != null){
                    StringTokenizer itr = new StringTokenizer(line);
                    String cur_word;
                    Integer cur_idf;

                    if(itr.hasMoreTokens()){
                        cur_word = itr.nextToken();
                        if (itr.hasMoreTokens()) {
                            cur_idf = Integer.parseInt(itr.nextToken().replaceAll("[^0-9]", ""));
                            get_idf.put(cur_word, cur_idf);
                        }
                    }

                    line = reader.readLine(); // reading the next line
                }

            }
        }

        public void reduce(Text key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            MapWritable result = new MapWritable();

            Integer length = 0;
            for (MapWritable map: values) {
                Set<Writable> keys = map.keySet();
                for(Writable k: keys){
                    String val_word = k.toString();
                    Integer tf = ((IntWritable)(map.get(k))).get();

                    length = length + tf;


                    Integer idf = get_idf.get(val_word);

                    if (idf != null) {
                        Float tfidf = (float)tf / (float)idf;
                        result.put(new IntWritable(val_word.hashCode()), new FloatWritable(tfidf));
                    }

                }
            }

            context.write(new Text(key.toString() + " length: " + length.toString()), result);
        }
    }



    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        // validating arguments
        if (args.length != 2) {
            System.out.println("Program is epecting two arguments");
            System.exit(1);
        }

        if (args[1].equals("idf_result")) {
            System.out.println("this folder name is reserved for other purpose, choose another nane");
            System.exit(1);
        }

        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        if (!fs.exists(inputPath)) {
            System.out.println("unable to locate input directory");
            System.exit(1);
        }

        if (fs.exists(outputPath)) {
            System.out.println("out directory should not be created");
            System.exit(1);
        }

        // idf map-reduce task configuring classes
        Job idfTask = Job.getInstance(conf, "tf-idf");
        idfTask.setJarByClass(Indexer.class);
        idfTask.setMapperClass(IDFVectorMap.class);
        idfTask.setCombinerClass(IDFVectorReduce.class);
        idfTask.setReducerClass(IDFVectorReduce.class);
        idfTask.setOutputKeyClass(Text.class);
        idfTask.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputDirRecursive(idfTask, true);
        FileInputFormat.addInputPath(idfTask, new Path(args[0]));
        Path path = new Path("idf_result");
        FileOutputFormat.setOutputPath(idfTask, path);
        idfTask.waitForCompletion(true);





        conf = new Configuration();

        // initializing word to vec
        Job vecJob = Job.getInstance(conf, "vectorizer");


        fs = FileSystem.get(conf);
        try {


            FileStatus[] fileStatuses = fs.listStatus(new Path("idf_result"));

            // for each file in directory
            for(FileStatus fstat: fileStatuses) {
                String filename = fstat.getPath().toString();
                if (!filename.contains("SUCCESS")) { // ignoring success file

                    path = new Path("idf_result/" +
                            filename.substring(filename.indexOf("idf_result/") + "idf_result/".length()));
                    vecJob.addCacheFile(path.toUri());
                }

            }


        } catch (IOException e){
            e.printStackTrace();
        }

        
        vecJob.setJarByClass(Indexer.class);
        vecJob.setMapperClass(VecMap.class);
        vecJob.setReducerClass(VecReduce.class);
        vecJob.setOutputKeyClass(Text.class);
        vecJob.setOutputValueClass(MapWritable.class);
        FileInputFormat.setInputDirRecursive(vecJob, true);
        FileInputFormat.addInputPath(vecJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(vecJob, new Path(args[1]));

        // mapreduce job initiation
        vecJob.waitForCompletion(true);

        System.exit(1);

    }
}
