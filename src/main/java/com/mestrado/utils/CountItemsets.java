package main.java.com.mestrado.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import main.java.com.mestrado.main.Main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

public class CountItemsets {
	private static Integer itemsetsCounts[];

	public static void countByOutputDir(String outputPath) {
		Path path = new Path(outputPath);
		
		Configuration c = new Configuration();
		try {
            c.set("fs.defaultFS", "hdfs://master/");
			FileSystem fs = FileSystem.get(c);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(path)));
			String line;
			String[] lineSpt;
			while ((line = br.readLine()) != null) {
				lineSpt = line.split("\\s+");
				if(itemsetsCounts[lineSpt.length-2] == null){
					itemsetsCounts[lineSpt.length-2] = new Integer(1);
				}else{
					itemsetsCounts[lineSpt.length-2]++;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void countBySequence(String outputPath){
		Path path = new Path(outputPath);
		Configuration c = new Configuration();
		c.set("fs.defaultFS", "hdfs://master/");
		try {
			SequenceFile.Reader reader = new SequenceFile.Reader(c, SequenceFile.Reader.file(path));
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), c);
			IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), c);
			String[] lineSpt;
			while (reader.next(key, value)) {
				lineSpt = key.toString().split("\\s+");
				if(itemsetsCounts[lineSpt.length-1] == null){
					itemsetsCounts[lineSpt.length-1] = new Integer(1);
				}else{
					itemsetsCounts[lineSpt.length-1]++;
				}
	        }
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static String countItemsets() {
		itemsetsCounts = new Integer[20];
		ArrayList<String> outputFileNames;
		for(int i = 1; i <= Main.countDir; i++){
			outputFileNames = MrUtils.getAllOuputFilesNames(Main.user+"output"+i);
			for(String outFile : outputFileNames){
				System.out.println("Contando itemsets em "+outFile);
				CountItemsets.countByOutputDir(outFile);
			}
			if(i >= 3){
				outputFileNames = MrUtils.getAllSequenceFilesNames(Main.fileCachedDir,i);
				for(String outFile : outputFileNames){
					System.out.println("Contando itemsets em "+outFile);
					CountItemsets.countBySequence(outFile);
				}
			}
		}
		StringBuilder sb = new StringBuilder();
		int total = 0;
		for(int i = 0; i < itemsetsCounts.length; i++){
			if(itemsetsCounts[i] != null){
				total+=itemsetsCounts[i];
				sb.append((i + 1)).append("-itemsets: ").append(itemsetsCounts[i]).append("\n");
				System.out.println("Itemsets de tamanho "+(i+1)+": "+itemsetsCounts[i]);
			}
		}
		sb.append("total: ").append(total);
		System.out.println("Total: " + total);
		return sb.toString();
	}
}
