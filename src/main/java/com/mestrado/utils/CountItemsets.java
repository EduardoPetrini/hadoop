package main.java.com.mestrado.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import main.java.com.mestrado.main.Main;

public class CountItemsets {
	private static Integer itemsetsCounts[];
	private static ArrayList<String>[] realItemsets;

	private static void countByOutputDir(String outputPath) {
		Path path = new Path(outputPath);

		Configuration c = new Configuration();
		try {
			c.set("fs.defaultFS", "hdfs://master/");
			FileSystem fs = FileSystem.get(c);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line;
			String[] lineSpt;
			while ((line = br.readLine()) != null) {
				lineSpt = line.split("\\s+");
//				if(realItemsets[lineSpt.length - 2] == null){
//					realItemsets[lineSpt.length - 2] = new ArrayList<String>();
//				}
//				realItemsets[lineSpt.length - 2].add(line);
				if (itemsetsCounts[lineSpt.length - 2] == null) {
					itemsetsCounts[lineSpt.length - 2] = new Integer(1);
				} else {
					itemsetsCounts[lineSpt.length - 2]++;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String countItemsets() {
		itemsetsCounts = new Integer[20];
		realItemsets = new ArrayList[20];
		ArrayList<String> outputFiles = MrUtils.getAllOuputFilesNames(Main.user + "output1");
		for (String outFile : outputFiles) {
			System.out.println("Contando itemsets em " + outFile);
			CountItemsets.countByOutputDir(outFile);
		}
		outputFiles = MrUtils.getAllOuputFilesNames(Main.user + "output2");
		for (String outFile : outputFiles) {
			System.out.println("Contando itemsets em " + outFile);
			CountItemsets.countByOutputDir(outFile);
		}
		int total = 0;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < itemsetsCounts.length; i++) {
			if (itemsetsCounts[i] != null) {
				total += itemsetsCounts[i];
				sb.append((i + 1)).append("-itemsets: ").append(itemsetsCounts[i]).append("\n");
				System.out.println("Itemsets de tamanho "+(i+1)+": "+itemsetsCounts[i]);
			}
		}
		sb.append("total: ").append(total);
		System.out.println("Total: " + total);
		return sb.toString();
	}
	
	public static void printRealItemsets(){
		for(int i = 0; i < itemsetsCounts.length; i++){
			if(itemsetsCounts[i] != null){
				System.out.println("\n********** "+(i+1)+" = "+itemsetsCounts[i]);
				for(String item: realItemsets[i]){
					System.out.println("****** "+item);
				}
			}
		}
	}
}
