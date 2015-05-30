package com.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

public class TextToMapFileConverter {

	@SuppressWarnings("deprecation")
	public static String converter(String path) {

		Configuration conf = new Configuration();
		FileSystem fs = null;
		MapFile.Writer writer = null;
		String outPath = path+"_output";
		try {
			FileSystem.get(conf);
			Path inputPath = new Path(path);
			Path outputPath = new Path(outPath);
			Text txtKey = new Text();
			Text txtValue = new Text();
			fs = FileSystem.get(conf);
			FSDataInputStream inputStrem = fs.open(inputPath);
			writer = new MapFile.Writer(conf, fs, outputPath.toString(),
					txtKey.getClass(), txtKey.getClass());
			
			writer.setIndexInterval(1);
			while(inputStrem.available()>0){
				String[] data = inputStrem.readLine().split(",");
				txtKey.set(data[0]);
				txtValue.set(data[1]);
				writer.append(txtKey, txtValue);
				
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		finally{
			IOUtils.closeStream(writer);
		}
		return outPath;

	}

}
