import com.amazonaws.services.s3.model.GetObjectRequest;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Scanner;


import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.core.ResponseBytes;


public class WorkJobs {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        try {
            /////// Zero job///////
           // System.setProperty("hadoop.home.dir", "C:\\Users\\reuty\\hadoop-2.6.0\\");//////////////////////////////
            //Configuration conf0 = new Configuration();

            S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
            //try {
            software.amazon.awssdk.services.s3.model.GetObjectRequest request = software.amazon.awssdk.services.s3.model.GetObjectRequest.builder().key(args[1]).bucket("ass2bucket2").build();
            ResponseBytes<GetObjectResponse> responseBytes = s3.getObjectAsBytes(request);
                byte[] data = responseBytes.asByteArray();
               String stopWords = new String(data, StandardCharsets.UTF_8);


     /*        Configuration conf0 = new Configuration();
            conf0.set("WordStopFile", stopWords);
            System.out.println("starting job 0");
            Job job0 = Job.getInstance(conf0,"preJob");
            job0.setJarByClass(CountNPerDecade.class);

            job0.setOutputKeyClass(Text.class);
            job0.setOutputValueClass(LongWritable.class);
            job0.setMapOutputKeyClass(Text.class);
            job0.setMapOutputValueClass(LongWritable.class);

            job0.setPartitionerClass(CountNPerDecade.PartitionerClass.class);
            job0.setMapperClass(CountNPerDecade.MapperClass.class);
            job0.setReducerClass(CountNPerDecade.ReducerClass.class);
            job0.setCombinerClass(CountNPerDecade.CombinerClass.class);

            FileInputFormat.addInputPath(job0, new Path(args[2])); // path need to be with one grams.
            FileOutputFormat.setOutputPath(job0, new Path("s3://ass2bucket2/output1"));


            job0.setInputFormatClass(SequenceFileInputFormat.class);
            //job0.setInputFormatClass(TextInputFormat.class);
            job0.setOutputFormatClass(TextOutputFormat.class);

/*
             job0.waitForCompletion(true);
             if (job0.isSuccessful()) {
                 System.out.println("Finish the pre job");
               } else {
               throw new RuntimeException("Job failed : " + job0);
              }



            /////// first job///////
         //   Configuration conf1 = new Configuration();
         //   conf1.set("WordStopFile", stopWords); // stopwords in args[0]
          //  String pathOutputPreJob = "";
         //   String decadeTable = "";
            for(int i=0; i<=26; i++){
                if(i<10){ pathOutputPreJob = "s3://ass2bucket2/output1/part-r-0000"+""+"0"+i; }
                else{  pathOutputPreJob = "s3://ass2bucket2/output1/part-r-0000"+""+i;  }
          //      Path path = new Path(pathOutputPreJob);
           //     FileSystem fs = path.getFileSystem(conf1);
           //     FSDataInputStream inputStream = fs.open(path);
           //     String tmpdacadeTable = IOUtils.toString(inputStream,"UTF-8");
             //   System.out.println(tmpdacadeTable);
                decadeTable = decadeTable+tmpdacadeTable;
            //    System.out.println("The decadeTable After adding:\n"+decadeTable);
                fs.close();
            }

        //    conf1.set("decadeTable",decadeTable);
          //  conf1.reloadConfiguration();
         //   System.out.println("starting job 1");
         //   Job job1 = Job.getInstance(conf1, "firstJob");
          //  job1.setJarByClass(FirstJob.class);
          //  job1.setOutputKeyClass(KeyWordPerDecade.class);
           // job1.setOutputValueClass(ValueForFirstJob.class);
            job1.setMapOutputKeyClass(KeyWordPerDecade.class);
            job1.setMapOutputValueClass(LongWritable.class);

            job1.setMapperClass(FirstJob.MapperClass.class);
            job1.setReducerClass(FirstJob.ReducerClass.class);
            job1.setCombinerClass(FirstJob.CombinerClass.class);
            job1.setPartitionerClass(FirstJob.PartitionerClass.class);
             //job1.setInputFormatClass(TextInputFormat.class);
           // job1.setInputFormatClass(SequenceFileInputFormat.class);

          // MultipleInputs.addInputPath(job1, new Path(args[2]),SequenceFileInputFormat.class);
            FileInputFormat.addInputPath(job1, new Path(args[2]));
         //  MultipleInputs.addInputPath(job1, new Path("s3://ass2bucket2/output1"),TextInputFormat.class);
           //SequenceFileInputFormat.addInputPath(job1, new Path(args[2])); // path need to be with one grams.
           // SequenceFileInputFormat.addInputPath(job1, new Path("s3://ass2bucket2/output1"));
            FileOutputFormat.setOutputPath(job1, new Path("s3://ass2bucket2/output2"));  //the path from s3 need to be change"s3n://ass02/Step1"
            job1.setInputFormatClass(SequenceFileInputFormat.class);
            //job1.setInputFormatClass(TextInputFormat.class);
            job1.setOutputFormatClass(TextOutputFormat.class);


             job1.waitForCompletion(true);
             if (job1.isSuccessful()) {
                System.out.println("Finish the first job");
              } else {
                throw new RuntimeException("Job failed : " + job1);
              }
 /*
      */      ////job2/////
               Configuration conf2 = new Configuration();
             conf2.set("WordStopFile", stopWords); // stopwords in args[0]
            System.out.println("starting job 2");
            Job job2 = Job.getInstance(conf2,"SecondJob");
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
            FileOutputFormat.setOutputPath(job2,new Path("s3://ass2bucket2/output3"));  //the path from s3 need to be change
           /// job2.setInputFormatClass(SequenceFileInputFormat.class);
          //  job2.setInputFormatClass(TextInputFormat.class);///////////////////////
            job2.setInputFormatClass(SequenceFileInputFormat.class);

            job2.setOutputFormatClass(TextOutputFormat.class);

              job2.waitForCompletion(true);
              if (job2.isSuccessful()){
                  System.out.println("Finish the second job");
              }
             else {
               throw new RuntimeException("Job failed : " + job2);
             }
          /////job3 - join ////
            System.out.println("starting job 3");
            Configuration conf3 = new Configuration();
            Job job3 = Job.getInstance(conf3,"ThirdJob");
            job3.setJarByClass(ThirdJob.class);

            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);
            job3.setMapOutputKeyClass(KeyForFirstJoin.class);
            job3.setMapOutputValueClass(Text.class);

            job3.setMapperClass(ThirdJob.MapperClass.class);
            job3.setReducerClass(ThirdJob.ReducerClass.class);
            //job3.setCombinerClass(ThirdJob.CombinerClass.class);
            job3.setPartitionerClass(ThirdJob.PartitionerClass.class);
            //  job3.setNumReduceTasks(32);

          //  job3.setInputFormatClass(TextInputFormat.class);

            MultipleInputs.addInputPath(job3, new Path("s3://ass2bucket2/output3"),TextInputFormat.class); // path need to be with one grams.
            MultipleInputs.addInputPath(job3, new Path("s3://ass2bucket2/output2"),TextInputFormat.class);
            FileOutputFormat.setOutputPath(job3,new Path("s3://ass2bucket2/output4"));  //the path from s3 need to be change
            job3.setOutputFormatClass(TextOutputFormat.class);

             job3.waitForCompletion(true);
               if (job3.isSuccessful()){
                System.out.println("Finish the third job");
             }
             else {

               }      throw new RuntimeException("Job failed : " + job3);
               /*

            /////////////////////////// job4
            System.out.println("starting job 4");
            Configuration conf4 = new Configuration();
            Job job4 = Job.getInstance(conf4,"second join");
            job4.setJarByClass(JoinAllDetails.class);

            job4.setOutputKeyClass(Text.class);
            job4.setOutputValueClass(Text.class);
            job4.setMapOutputKeyClass(KeyForFirstJoin.class);
            job4.setMapOutputValueClass(Text.class);

            job4.setMapperClass(JoinAllDetails.MapperClass.class);
            job4.setReducerClass(JoinAllDetails.ReducerClass.class);
            //job3.setCombinerClass(ThirdJob.CombinerClass.class);
            job4.setPartitionerClass(JoinAllDetails.PartitionerClass.class);
            //  job3.setNumReduceTasks(32);

            //job4.setInputFormatClass(TextInputFormat.class);


            MultipleInputs.addInputPath(job4, new Path("s3://ass2bucket2/output2"),TextInputFormat.class); // path need to be with one grams.
            MultipleInputs.addInputPath(job4, new Path("s3://ass2bucket2/output4"),TextInputFormat.class);
            FileOutputFormat.setOutputPath(job4,new Path("s3://ass2bucket2/output5"));  //"s3://ass2bucket2/output5/" change
           // job4.setOutputFormatClass(TextOutputFormat.class);
            //job4.waitForCompletion(true);
            //  if (job4.isSuccessful()){
           //     System.out.println("Finish the second job");
           //    }
            //   else {
            //  throw new RuntimeException("Job failed : " + job4);
            // }

            /////////////////////

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
            job5.setNumReduceTasks(32);

           // job5.setInputFormatClass(TextInputFormat.class);


            FileInputFormat.addInputPath(job5, new Path("s3://ass2bucket2/output5")); // path need to be with one grams.
            FileOutputFormat.setOutputPath(job5,new Path(args[4]));  //"s3://ass2bucket2/output5/" change
            job5.setInputFormatClass(TextInputFormat.class);
            job5.setOutputFormatClass(TextOutputFormat.class);

            //if (job5.waitForCompletion(true)){
           //     System.out.println("finish all!");
           // }


            ControlledJob jobZeroControl = new ControlledJob(job0.getConfiguration());
            jobZeroControl.setJob(job0);
            ControlledJob jobOneControl = new ControlledJob(job1.getConfiguration());
            jobOneControl.setJob(job1);
/*
            ControlledJob jobTwoControl = new ControlledJob(job2.getConfiguration());
            jobTwoControl.setJob(job2);
            ControlledJob jobThreeControl = new ControlledJob(job3.getConfiguration());
            jobThreeControl.setJob(job3);

            ControlledJob jobFourControl = new ControlledJob(job4.getConfiguration());
            jobFourControl.setJob(job4);

            ControlledJob jobFiveControl = new ControlledJob(job5.getConfiguration());
            jobFiveControl.setJob(job5);

            JobControl jobControl = new JobControl("job-control");
            jobControl.addJob(jobZeroControl);
            jobControl.addJob(jobOneControl);
          /*  jobControl.addJob(jobThreeControl);
            jobControl.addJob(jobTwoControl);
            jobControl.addJob(jobFourControl);
            jobControl.addJob(jobFiveControl);

            jobOneControl.addDependingJob(jobZeroControl); // this condition makes the job-2 wait until job-1 is done
 /*           jobThreeControl.addDependingJob(jobOneControl);
            jobThreeControl.addDependingJob(jobTwoControl);
            jobFourControl.addDependingJob(jobThreeControl);
            jobFiveControl.addDependingJob(jobFourControl);
            //  jobFourControl.addDependingJob(jobTwoControl);

            Thread jobControlThread = new Thread(jobControl);
            jobControlThread.start();
            int code = 0;
            while (!jobControl.allFinished()) {
                code = jobControl.getFailedJobList().size() == 0 ? 0 : 1;
                Thread.sleep(1000);
            }
            for(ControlledJob j : jobControl.getFailedJobList())
                System.out.println(j.getMessage());
            System.exit(code);
*/
        }  catch (IOException | InterruptedException  e) {
            e.printStackTrace();
        }

    }
}
