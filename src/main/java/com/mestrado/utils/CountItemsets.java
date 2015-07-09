package main.java.com.mestrado.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import main.java.com.mestrado.main.Main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CountItemsets {
	private static Integer itemsetsCounts[];

	public static void countByOutputDir(String outputPath) {
		Path path = new Path(outputPath);
		
			Configuration c = new Configuration();
			try {
	          c.set("fs.defaultFS", Main.clusterUrl);
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
	
	public static void main(String[] args) {
		itemsetsCounts = new Integer[20];
		CountItemsets.countByOutputDir("/user/eduardo/output1/part-r-00000");
		CountItemsets.countByOutputDir("/user/eduardo/output2/part-r-00000");
		int total = 0;
		for(int i = 0; i < itemsetsCounts.length; i++){
			if(itemsetsCounts[i] != null){
				total+=itemsetsCounts[i];
				System.out.println("Itemsets de tamanho "+(i+1)+": "+itemsetsCounts[i]);
			}
		}
		
		System.out.println("Total: "+total);
	}
}
