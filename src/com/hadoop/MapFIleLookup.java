package com.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

public class MapFIleLookup {

	
	public static Text getValue(String key,String path){
		Configuration conf=new Configuration();
		FileSystem fs=null;
		Text txtKey=new Text(key);
		Text txtValue=new Text();
		MapFile.Reader reader=null;
		try {
			fs=FileSystem.get(conf);
			reader=new MapFile.Reader(fs, path.toString(), conf);
			reader.get(txtKey, txtValue);
			reader.close();
			System.out.println("key : "+txtKey+" ,value: "+txtValue);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return txtValue;
	}
	
	public static void main(String agrs[]){
		getValue("d00", "/home/gurdit/Hadoop_Data/MapFile_out");
	}
}
