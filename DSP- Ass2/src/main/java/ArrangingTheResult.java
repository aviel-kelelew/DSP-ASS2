import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class ArrangingTheResult {

    public static class MapperClass extends Mapper<LongWritable, Text, Probability, Text> {
        @Override
        //<<w1,W2,Decade><ratio>>
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] args = line.toString().split("\t");
            //<<decade,w1,w2,ratio><0>>
            double ratio = 0;
            if(!args[3].equals("NaN") && !args[3].equals("Infinity") ){ ratio =  Double.parseDouble(args[3]);}
            context.write(new Probability(Integer.valueOf(args[2]),args[0],args[1],ratio),new Text(""));
        }
}

    public static class ReducerClass extends Reducer<Probability, Text, Text, Text> {
        Integer count =100;


        public void reduce(Probability key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
          //  if(count == 100)  context.write(new Text("Decade is:"), new Text(String.valueOf(key.getDecade())));
            for(Text value: values){
             if(count > 0){
                // System.out.println("The last class:"+key.getWord1()+" "+key.getWord2()+" "+ key.getProbability());
                   context.write(new Text(key.getDecade()+" "+key.getWord1()+" "+key.getWord2()),new Text(String.valueOf(key.getProbability())));
                   count --;
             }
             else{return;}

        }
        }

        public void cleanup(Context context) {
        }

    }



    public static class PartitionerClass extends Partitioner<Probability, Text> {
        @Override
        public int getPartition(Probability key, Text text, int numPartitions) {
            return key.ghashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("starting job 5");
        Configuration conf5 = new Configuration();
        Job job5 = Job.getInstance(conf5,"last job");
        job5.setJarByClass(ArrangingTheResult.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);
        job5.setMapOutputKeyClass(Probability.class);
        job5.setMapOutputValueClass(Text.class);

        job5.setMapperClass(ArrangingTheResult.MapperClass.class);
        job5.setReducerClass(ArrangingTheResult.ReducerClass.class);
        job5.setPartitionerClass(ArrangingTheResult.PartitionerClass.class);
        job5.setNumReduceTasks(34);

        // job5.setInputFormatClass(TextInputFormat.class);


        FileInputFormat.addInputPath(job5, new Path("s3://ass2bucket1/output5")); // path need to be with one grams.
        FileOutputFormat.setOutputPath(job5,new Path(args[4]));  //"s3://ass2bucket2/output5/" change
        job5.setInputFormatClass(TextInputFormat.class);
        job5.setOutputFormatClass(TextOutputFormat.class);

        if (job5.waitForCompletion(true)){
             System.out.println("finish all!");
         }
    }
}