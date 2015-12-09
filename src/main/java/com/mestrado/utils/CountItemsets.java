package main.java.com.mestrado.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import main.java.com.mestrado.main.Main;
import main.java.com.mestrado.main.MainSpark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CountItemsets {
	private static Integer itemCounts[];
	private static ArrayList<String>[] realItemsets;

	private static void countByOutputDir(String outputPath) {
		Path path = new Path(outputPath);

		Configuration c = new Configuration();

		try {
			c.set("fs.defaultFS", MainSpark.clusterUrl);
			FileSystem fs = FileSystem.get(c);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line;
			String itemSup;
			String[] lineSpt;
			while ((line = br.readLine()) != null) {
				itemSup = line.replaceAll("\\(||\\)", "");
				lineSpt = itemSup.replaceAll(",.*", "").split(" ");
				
//				if(realItemsets[lineSpt.length-1] == null){
//					realItemsets[lineSpt.length-1] = new ArrayList<String>();
//				}
//				realItemsets[lineSpt.length-1].add(itemSup);
				if (itemCounts[lineSpt.length - 1] == null) {
					itemCounts[lineSpt.length - 1] = new Integer(1);
					// sbs[lineSpt.length-1] = new StringBuilder();
					// sbs[lineSpt.length-1].append(l).append("\n");
				} else {
					itemCounts[lineSpt.length - 1]++;
					// sbs[lineSpt.length-1].append(l).append("\n");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String countItemsets(String... filesNames) {
		itemCounts = new Integer[20];
		realItemsets= new ArrayList[20];
		for (String dirName : filesNames) {
			ArrayList<String> outputFiles = MrUtils.getAllOuputFilesNames(dirName);
			for (String outFile : outputFiles) {
				System.out.println("Contando itemsets em " + outFile);
				CountItemsets.countByOutputDir(outFile);
			}
		}
		int total = 0;
		StringBuilder sb = new StringBuilder();
		sb.append("\n|**********************************|\n\n");
		for (int i = 0; i < itemCounts.length; i++) {
			if (itemCounts[i] != null) {
				total += itemCounts[i];
				sb.append("**********  ").append((i + 1)).append(": ").append(itemCounts[i]).append("\n");
			}
		}
		sb.append("total: ").append(total);
		sb.append("\n\n|**********************************|\n\n");
//		System.out.println("ITEMSETSHERE \n\n"+sb.toString());
		return sb.toString();
	}
	
	public static void printRealItemsets(){
		for (int i = 0; i < itemCounts.length; i++){
			if (itemCounts[i] != null) {
				Collections.sort(realItemsets[i],ITEMSET_ORDER);
				System.out.println("\n************** "+(i+1)+" : "+itemCounts[i]);
				for(String item: realItemsets[i]){
					System.out.println(item);
				}
			}
		}
	}
	
	public static Comparator<String> ITEMSET_ORDER = new Comparator<String>(){

		@Override
		public int compare(String o1, String o2) {
			String[] item1 = o1.replaceAll(",.*","").split(" ");
			String[] item2 = o2.replaceAll(",.*","").split(" ");
			
			for(int i = 0; i < item1.length; i++){
				if(Integer.parseInt(item1[i]) < Integer.parseInt(item2[i])){
					return -1;
				}else if(Integer.parseInt(item1[i]) > Integer.parseInt(item2[i])){
					return 1;
				}
			}
			return 0;
		}
		
	};
}
