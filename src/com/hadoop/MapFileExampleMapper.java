package com.hadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapFileExampleMapper {
	
	public static class MapFileMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		private MapFile.Reader reader=null;
		@Override
		public void setup(Context context) throws IOException{
			Configuration conf=context.getConfiguration();
			FileSystem fs=FileSystem.get(conf);
			Path[] path = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			String mapFile = TextToMapFileConverter.converter(path[0].toString());
			this.reader=new MapFile.Reader(fs, mapFile, conf);
		}
		
		private Text findKey(Text txt){
			Text val=new Text();
			try {
				reader.get(txt, val);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return val;
		}
		
		@Override
		public void map(LongWritable key,Text value,Context context){
			
			String line = value.toString();
			String[] fields = line.split(",");
			Text txtkey=new Text(fields[0]);
			Text txtvalue = findKey(txtkey);
			if(!txtvalue.equals("") || txtvalue!=null){
				try {
					context.write(new Text(fields[0]),new Text(txtvalue.toString()+"\t"+fields[1]));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		public void cleanup(Context context) throws IOException{
	        reader.close();
	    }
	}

	
	public static void main(String args[]) throws Exception{
		Job job=new Job();
		job.setJarByClass(MapFileExampleMapper.class);
		job.setJobName("Lookup");
		
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job,new Path(args[2]));
		Configuration conf = job.getConfiguration();
		DistributedCache.addCacheFile(new URI(args[0]), conf);
		job.setMapperClass(MapFileMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}

}
