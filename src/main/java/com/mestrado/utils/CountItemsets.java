package main.java.com.mestrado.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import main.java.com.mestrado.main.MainSpark;

public class CountItemsets {
	private static Integer itemsetsCounts[];

	public static int countByOutputDir(String outputPath) {
		Path path = new Path(outputPath);
		int numLines = 0;
		Configuration c = new Configuration();
		try {
            c.set("fs.defaultFS", MainSpark.clusterUrl);
			FileSystem fs = FileSystem.get(c);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(path)));
			String line;
			while ((line = br.readLine()) != null) {
				numLines++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return numLines;
	}
	
	public static int countBySequence(String outputPath){
		Path path = new Path(outputPath);
		Configuration c = new Configuration();
		int numLines = 0;
		c.set("fs.defaultFS", MainSpark.clusterUrl);
		try {
			SequenceFile.Reader reader = new SequenceFile.Reader(c, SequenceFile.Reader.file(path));
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), c);
			IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), c);
			String[] lineSpt;
			while (reader.next(key, value)) {
				numLines++;
	        }
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return numLines;
	}
	
	/*public static String countItemsets() {
		itemsetsCounts = new Integer[20];
		//obter todos os arquivos de cada diretório
		ArrayList<String> outputFileNames;
		for(int i = 1; i <= MainSpark.countDir; i++){
			outputFileNames = MrUtils.getAllOuputFilesNames(MainSpark.user + "output" + i);
			for(String outFile : outputFileNames){
				System.out.println("Contando itemsets em " + outFile);
				CountItemsets.countByOutputDir(outFile);
			}
		}
		StringBuilder sb = new StringBuilder();
		int total = 0;
		for(int i = 0; i < itemsetsCounts.length; i++){
			if(itemsetsCounts[i] != null){
				total += itemsetsCounts[i];
				sb.append((i + 1)).append("-itemsets: ").append(itemsetsCounts[i]).append("\n\t");
				System.out.println("Itemsets de tamanho " + (i + 1) + ": " + itemsetsCounts[i]);
			}
		}
		sb.append(total).append("\n");
		System.out.println("Total: " + total);
		return sb.toString();
	}*/
	
	public static String countItemsets() {
		itemsetsCounts = new Integer[20];
		//obter todos os arquivos de cada diretório
		ArrayList<String> outputFileNames;
		for(int i = 1; i <= MainSpark.countDir; i++){
			outputFileNames = MrUtils.getAllOuputFilesNames(MainSpark.user + "output" + i);
			for(String outFile : outputFileNames){
				System.out.println("Contando itemsets em " + outFile);
				CountItemsets.countByOutputDir(outFile);
			}
		}
		StringBuilder sb = new StringBuilder();
		int total = 0;
		for(int i = 0; i < itemsetsCounts.length; i++){
			if(itemsetsCounts[i] != null){
				total += itemsetsCounts[i];
				sb.append((i + 1)).append("-itemsets: ").append(itemsetsCounts[i]).append("\n\t");
				System.out.println("Itemsets de tamanho " + (i + 1) + ": " + itemsetsCounts[i]);
			}
		}
		sb.append(total).append("\n");
		System.out.println("Total: " + total);
		return sb.toString();
	}
	
	public static void countSparkItemsets() {
		itemsetsCounts = new Integer[20];
		int numItemSets = 0;
		int total = 0;

		ArrayList<String> outputFileNames;
		for(int i = 1; i <= MainSpark.countDir; i++){
			numItemSets = 0;
			outputFileNames = MrUtils.getAllOuputFilesNames(MainSpark.user + "inputCandidates/" + "C" + i);
			for(String outFile : outputFileNames){
				//System.out.println("Contando itemsets em " + outFile);
				numItemSets += CountItemsets.countBySequence(outFile);					
			}
			MrUtils.appendToFile(MainSpark.durationLogName, "Itemsets de tamanho " + i + ": " + numItemSets);
			System.out.println("Itemsets de tamanho " + i + ": " + numItemSets);
			total += numItemSets;
		}
		MrUtils.appendToFile(MainSpark.durationLogName, "Total: " + total);
		System.out.println("Total: " + total);
	}
}
