import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Probelistic Model
 */
public class Indexer {

    /**
     * Mapper class
     */
    public static class DocsMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        /**
         * Map method
         * @param key key
         * @param value value
         * @param context context
         * @throws IOException IOException
         * @throws InterruptedException InterruptedException
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString(); // reading a ling containing json about doc
            String text = line.substring(line.indexOf(", \"text\":") + 9); // acquiring "text" part of json

            StringTokenizer textToTokenIterator = new StringTokenizer(text.toLowerCase()
                    .replaceAll("\\\\[a-z]", " ")
                    .replaceAll("-", " ")); //tokenizing text, removing symbols of tabulation and "-"

            line = null;
            text = null;

            String currentToken; // current token
            HashSet storedTokens = new HashSet(); // stores already processed tokens (words)

            // iterating through tokens
            while (textToTokenIterator.hasMoreTokens()) {
                // getting rid of non-letter symbols
                currentToken = textToTokenIterator.nextToken().replaceAll("[\\\\0-9~`!@#$%^&*()\\-_+=\\,.<>?/'\":;{}\\[\\]\\|]", "");
                if (! currentToken.equals("")) { // non-empty token
                    if (!storedTokens.contains(currentToken)) { // 1st occurrence
                        storedTokens.add(currentToken); // adding to processed tokens
                        word.set(currentToken);
                        context.write(word, one); // passing to reducer
                    }
                }
            }
        }
    }

    public static class DocsReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0; // # of docs in which the word has occurred
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static class IndexMapper
            extends Mapper<Object, Text, Text, MapWritable>{

        private Text word;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString(); // acquiring the doc
            String text = line.substring(line.indexOf(", \"text\":") + 9); // getting doc text
            String id = line.substring(8, line.indexOf("\", \"url\"")); // getting doc id

            StringTokenizer textToTokenIterator = new StringTokenizer(text.toLowerCase().
                    replaceAll("\\\\[a-z]", " ")
                    .replaceAll("-", " ")); //tokenizing text, removing symbols of tabulation and "-"

            String currentToken; // current token


            text = null;
            line = null;

            MapWritable map = new MapWritable(); // <word, # of its occurrences in the text>

            // iterating through text
            while (textToTokenIterator.hasMoreTokens()) {
                // getting rid of non-letter symbols
                currentToken = textToTokenIterator.nextToken().replaceAll("[\\\\0-9~`!@#$%^&*()\\-_+=\\,.<>?/'\":;{}\\[\\]\\|]", "");
                if (! currentToken.equals("")) { //not empty tokens
                    word = new Text(currentToken);
                    if (! map.containsKey(word)) {
                        map.put(word, new IntWritable(1));
                    }
                    else {
                        IntWritable r = (IntWritable) map.get(word);
                        map.put(word, new IntWritable(r.get() + 1));
                    }
                }
            }

            context.write(new Text(id), map); // passing to reducer
        }
    }

    public static class IndexReducer
            extends Reducer<Text, MapWritable, Text, MapWritable> {

        public void reduce(Text key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            MapWritable result = new MapWritable(); // <hash of word, tf-idf of word>

            Integer length = 0; // # of words in doc text
            for (MapWritable map: values) {
                Set<Writable> keys = map.keySet(); // getting all words that occurred in text
                for(Writable k: keys){
                    String word = k.toString();
                    Integer tf = ((IntWritable)(map.get(k))).get(); // tf of a word in the given doc

                    length = length + tf; // updating the length of the doc
                    result.put(new IntWritable(word.hashCode()), new FloatWritable(tf));

                }
            }

            context.write(new Text(key.toString() + " length: " + length.toString()), result); // writing the result
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        // CHECKING ARGUMENTS CORRECTNESS
        if (args.length != 2) {
            System.out.println("The number of arguments provided is incorrect");
            System.out.println("---------------------------------");
            System.exit(1);
        }

        if (args[1].equals("output_docsww")) {
            System.out.println("Invalid folder name. You can call your output folder by any other name");
            System.out.println("---------------------------------");
            System.exit(1);
        }

        FileSystem fileSystem = FileSystem.get(conf);
        Path p1 = new Path(args[0]);
        Path p2 = new Path(args[1]);

        if (!fileSystem.exists(p1)) {
            System.out.println("The input directory doesn't exist");
            System.out.println("---------------------------------");
            System.exit(1);
        }

        if (fileSystem.exists(p2)) {
            System.out.println("The output folder has to be the one that does not exist yet");
            System.out.println("---------------------------------");
            System.exit(1);
        }


        ///////////// Counting # of docs in which each word has occurred /////////////
        Job job = Job.getInstance(conf, "Docs With Word");
        job.setJarByClass(Indexer.class);
        job.setMapperClass(DocsMapper.class);
        job.setReducerClass(DocsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path path = new Path("output_docsww");
        FileOutputFormat.setOutputPath(job, path);
        job.waitForCompletion(true);
        ///////////// Counting # of docs in which each word has occurred  /////////////


        ///////////// Word2Vec & TF /////////////
        // setting configs
        conf = new Configuration();

        // initializing job
        Job job2 = Job.getInstance(conf, "indexer");


        // setting corresponding classes
        job2.setJarByClass(Indexer.class);
        job2.setMapperClass(IndexMapper.class);
        job2.setReducerClass(IndexReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(MapWritable.class);



        // Files
        FileInputFormat.setInputDirRecursive(job2, true);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        // Starting the job
        job2.waitForCompletion(true);

        ///////////// AVG DOCs LEN /////////////
        int n_docs = 0; // # of documents
        int sum = 0; // sum of lens of all docs
        float avg = 0; // avg len of docs
        fileSystem = FileSystem.get(conf);
        BufferedReader reader;
        try {

            // listing filenames in the dir
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(args[1]));

            // going through each file
            for(FileStatus status: fileStatuses) {
                String filename = status.getPath().toString();
                if (!filename.contains("SUCCESS")) {
                    // reading files
                    path = new Path(args[1] + "/" + filename.substring(filename.indexOf(args[1]) + args[1].length() + 1));

                    reader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));

                    String line = reader.readLine();
                    while(line != null){
                        if(line.length() > 0) {
                            n_docs ++; // incrementing # of docs
                            // getting len of the doc
                            sum += Integer.parseInt(line.substring(line.indexOf("length: ") + "length: ".length(), line.indexOf("{")).replaceAll("[^0-9]", ""));
                        }
                        line = reader.readLine(); // reading the next line
                    }
                }
            }
            // calculating the avg len
            avg = (float) sum / (float) n_docs;

            FSDataOutputStream out = fileSystem.create(new Path(args[1] + "/avg_len"));
            out.writeChars(Float.toString(avg) + "  " + Integer.toString(n_docs));

        } catch (IOException e){
            e.printStackTrace();
        }
        ///////////// AVG DOCs LEN  /////////////


        System.exit(1);

    }
}