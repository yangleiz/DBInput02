package com.baizhi.yhz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


/**
 * Created by YHZ on 2018/7/25.
 */
public class TestDBInputFormat  {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        //1.封装Job对象-任务
        Configuration conf=new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("mapred-site.xml");
        conf.addResource("yarn-site.xml");


        conf.set("tmpjars","file:///D:\\apache-maven-3.3.9-bin\\maven\\mysql\\mysql-connector-java\\5.1.41\\mysql-connector-java-5.1.41.jar");
        conf.set("mapreduce.job.jar","file:///D:\\ideaWork\\finaltest\\DBInput02\\target\\DBInput02-1.0-SNAPSHOT.jar");


        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.0.50:3306/student",
                "root",
                "123");
        Job job = Job.getInstance(conf);
//        2.创建文件系统
        Path dist=new Path("/demo/dist");//结果目录必须为null
        TextOutputFormat.setOutputPath(job,dist);



        FileSystem fs=FileSystem.get(conf);
        if(fs.exists(dist)){
            fs.delete(dist,true);
        }
//        4.设置输入数据格式化的类和设置数据来源
        job.setInputFormatClass(DBInputFormat.class);

        DBInputFormat.setInput(job,User.class,"user",null, null, new String[]{"id","name"});
        //DBInputFormat.setInput(job, Student.class, "student", null, null, new String[]{"id","name"});
//    5.设置自定义的Mapper类和Mapper输出的key和value的类型
        job.setMapperClass(MyDBInputFormatMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//       6. 设置分区和reduce数量(reduce的数量和分区的数量对应，因为分区只有一个，所以reduce的个数也设置为一个)所以reduce的个数也设置为一个
        job.setReducerClass(MyDBInputFormatReduces.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        提交作业 然后关闭虚拟机正常退出
        job.setOutputFormatClass(TextOutputFormat.class);

        //提交
       // job.submit();
        job.waitForCompletion(true);

    }

    public static class MyDBInputFormatMapper extends Mapper<LongWritable,User,Text,Text>{
        //创建map输出时的key类型
        private Text mapOutKey = new Text();
        //创建map输出时的value类型
        private Text mapOutValue = new Text();
        @Override
        protected void map(LongWritable key, User value, Context context) throws IOException, InterruptedException {

            mapOutKey=new Text(value.getId().toString());
            mapOutValue=new Text(value.getName());

            context.write(mapOutKey,mapOutValue);
        }
    }

    public static class MyDBInputFormatReduces extends Reducer<Text,Text,Text,Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key,value);
            }
        }
    }


}
