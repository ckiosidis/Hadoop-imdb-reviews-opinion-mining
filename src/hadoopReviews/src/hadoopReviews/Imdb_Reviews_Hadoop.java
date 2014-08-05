package hadoopReviews;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.regex.*;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.cli.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Imdb_Reviews_Hadoop {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable> {


        private Text word = new Text();
        private String localname;
        private HashMap<String, float[]> Dictionary;
        private Stemmer s;


        // removes words that occur more than 150 times in the files
        // prepositions etc
        public String reg_remove(String text) {

            Pattern pattern = Pattern.compile("(?:\\b(?:their|they|we|you|your|my|the|a|about|by|for|to|is|are|then|when|where|who|will|was|what|[0-9]+)\\b)|(?:!|\"|-|--|\\?|`|#|@|$|/|\\.|:|;)|(?:\\b(?:action|after|all|also|an|and|another|any|around|as|at|audience|back|bad|be|because|been|before|being|best|better|big|but|by|can|character|characters|could|did|director|do|does|end|enough|even|every|few|film|films|first|from|get|go|going|good|great|had|has|have|he|her|here|him|his|how|however|i|if|in|into|it|its|just|know|life|like|little|love|made|make|makes|man|many|me|more|most|movie|movies|much|my|never|new|no|not|now|of|off|on|one|only|or|other|out|over|people|plot|really|scene|scenes|script|see|seems|she|should|so|some|something|still|story|such|than|that|their|them|there|these|they|thing|things|this|those|though|through|time|too|two|up|us|very|way|we|well|were|which|while|why|with|work|would|you)\\b)");
            return pattern.matcher(text).replaceAll("");
        }

        // reads the sentiwordnet.txt file and creates a HashMap<String,Float[]>
        // the float[] array contains the positive value at [0] and the negative at [1]
        public void readDic() throws FileNotFoundException, IOException {

            File file = new File("/usr/local/hadoop/hadoop-0.20.2/sentiwordnet.txt");
            FileInputStream fstream = new FileInputStream(file);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            String strArr[];

            while ((strLine = br.readLine()) != null) {       
                float[] temp = new float[2];
                if (strLine.trim().length() == 0) {           
                    continue;
                }
                strArr = strLine.split("\t");
                try {
                    temp[0] = Float.parseFloat(strArr[1]);    
                    temp[1] = Float.parseFloat(strArr[2]);
                } catch (NumberFormatException ex) {
                    continue;
                }
                Dictionary.put(strArr[3], temp);
            }
        }


        // initiate the variables
        public void setup(Context context) throws InterruptedException, IOException {
            super.setup(context);
            localname = ((FileSplit) context.getInputSplit()).getPath().toString();
            Dictionary = new HashMap<String, float[]>();
            readDic();
            s = new Stemmer();
        }



        // mapper
        // creates an entry of localname + reviewname
        // the float value is the value of the line of each string according to the line parsed
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());    
            String nextToken = "";                                                    
            String name = "";
            float sum = 0;
            if (itr.hasMoreTokens()) {
                name = itr.nextToken();
                while (itr.hasMoreTokens()) {
                    nextToken = s.mainfunction(reg_remove(itr.nextToken()));                  // calls the stemmer function and regular expression
                    try {
                        sum += Dictionary.get(nextToken)[0] - Dictionary.get(nextToken)[1];
                    } catch (Exception e) {
                        ;
                    }

                }
                word.set(localname + "/" + name);
                context.write(word, new FloatWritable(sum));
            }
        }
    }


    // reducer
    // for every entry
    // sums up the array values
    // outputs the sum
    public static class IntSumReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values,
                Context context) throws IOException, InterruptedException {
            Text word = new Text();
            word.set(key);
            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
            }
            context.write(word, new FloatWritable(sum));
        }
    }



    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "reviews");
        job.setJarByClass(Imdb_Reviews_Hadoop.class);
        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        System.out.println(job.waitForCompletion(true));

    }
}

