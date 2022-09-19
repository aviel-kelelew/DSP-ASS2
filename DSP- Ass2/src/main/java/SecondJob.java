import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SecondJob {
    private static HashSet<String> stopWordsHashSet = new HashSet<>();

    public static class MapperClass extends Mapper<LongWritable, Text, KeyWordPerDecade, LongWritable> {

        public void setup(Context context) throws IOException, InterruptedException{
            String wordStop=  context.getConfiguration().get("WordStopFile");
            try{
                for(String word : wordStop.split("\n")){
                    word = word.replace("\r","");
                    stopWordsHashSet.add(word);
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }
        public void map(LongWritable lineId, Text value, Context context) throws IOException, InterruptedException {
            //log.info("In map method");
            int year = 0;
            String[] line = value.toString().split("\t");
            if (line.length == 5) {
                String[] word = line[0].split(" ");
                if (word.length == 2) {
                    String firstWord = word[0].replaceAll(" ", "");
                    String secondWord = word[1].replaceAll(" ", "");
                    if (firstWord.length() >= 2 && secondWord.length() >= 2 && !stopWordsHashSet.contains(firstWord.toLowerCase()) && !stopWordsHashSet.contains(secondWord.toLowerCase())) {
                        try {
                            year = Integer.parseInt(line[1]);
                        } catch (Exception e) {
                            System.out.println(e.toString() + " secondJob map ->IntegerPArseInt");
                        }

                        context.write(new KeyWordPerDecade(year, firstWord, secondWord), new LongWritable(Long.parseLong(line[2])));
                    }

                }
            }
        }
    }

    //Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    public static class CombinerClass extends Reducer<KeyWordPerDecade, LongWritable, KeyWordPerDecade, LongWritable> {
        private long numberOfOcc;
        private String t;

        public void setup(Context context) {
            numberOfOcc = 0;
            t = "";
        }

        public void reduce(KeyWordPerDecade key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable value : values) {
                if (!key.toString().equals(t)) {
                    t = key.toString();
                    numberOfOcc = 0;
                }

                numberOfOcc = numberOfOcc + value.get();
            }

            context.write(key, new LongWritable(numberOfOcc));
        }

        public void cleanup(Context context) {
        }
    }

    public static class ReducerClass extends Reducer<KeyWordPerDecade, LongWritable, KeyWordPerDecade, ValueForFirstJob> {
        private long numberOfOcc;
        private String t;

        public void setup(Context context) {
            numberOfOcc = 0;
            t = "";

        }

        public void reduce(KeyWordPerDecade key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable value : values) {
                if (!key.toString().equals(t)) {
                    t = key.toString();
                    numberOfOcc = 0;
                }
                numberOfOcc = numberOfOcc + value.get();

            }
            context.write(key, new ValueForFirstJob(numberOfOcc));
        }

        public void cleanup(Context context) {
        }
    }

    public static class PartitionerClass extends Partitioner<KeyWordPerDecade, LongWritable> {
        public int getPartition(KeyWordPerDecade key, LongWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
        software.amazon.awssdk.services.s3.model.GetObjectRequest request = software.amazon.awssdk.services.s3.model.GetObjectRequest.builder().key(args[1]).bucket("ass2bucket1").build();
        ResponseBytes<GetObjectResponse> responseBytes = s3.getObjectAsBytes(request);
        byte[] data = responseBytes.asByteArray();
        String stopWords = new String(data, StandardCharsets.UTF_8);

        Configuration conf2 = new Configuration();
        conf2.set("WordStopFile", stopWords); // stopwords in args[0]
        System.out.println("starting job 2");
        Job job2 = Job.getInstance(conf2, "SecondJob");
        job2.setJarByClass(SecondJob.class);

        job2.setOutputKeyClass(KeyWordPerDecade.class);
        job2.setOutputValueClass(ValueForFirstJob.class);
        job2.setMapOutputKeyClass(KeyWordPerDecade.class);
        job2.setMapOutputValueClass(LongWritable.class);

        job2.setMapperClass(SecondJob.MapperClass.class);
        job2.setReducerClass(SecondJob.ReducerClass.class);
        job2.setCombinerClass(SecondJob.CombinerClass.class);
        job2.setPartitionerClass(SecondJob.PartitionerClass.class);


        //job2.setInputFormatClass(TextInputFormat.class);


        SequenceFileInputFormat.addInputPath(job2, new Path(args[3])); // path need to be with one grams.
        //FileInputFormat.addInputPath(job2, new Path(args[3]));//////////////////////////
        FileOutputFormat.setOutputPath(job2, new Path("s3://ass2bucket1/output3"));  //the path from s3 need to be change
        /// job2.setInputFormatClass(SequenceFileInputFormat.class);
        //  job2.setInputFormatClass(TextInputFormat.class);///////////////////////
        job2.setInputFormatClass(SequenceFileInputFormat.class);

        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.waitForCompletion(true);
        if (job2.isSuccessful()) {
            System.out.println("Finish the second job");
        } else {
            throw new RuntimeException("Job failed : " + job2);
        }
    }
}

