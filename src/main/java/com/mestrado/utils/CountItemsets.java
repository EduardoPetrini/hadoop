package main.java.com.mestrado.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;


import main.java.com.mestrado.main.Main;

public class CountItemsets {
	private static Integer itemsetsCounts[];
	private static ArrayList<String>[] realItemsets;
	private static HashMap<String,Integer> newItemsets;
	private static HashSet<String> itemsets;

	private static void countByOutputDir(Configuration c, String outputPath) {
		Path path = new Path(outputPath);

		
		try {
			
			FileSystem fs = FileSystem.get(c);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line;
			String[] lineSpt;
			while ((line = br.readLine()) != null) {
				lineSpt = line.split("\\s+");
				addItemset(line.trim());
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

	private static void countBySequenceDir(Configuration c, String sequencePath) {
		SequenceFile.Reader reader;
		Text key;
		String keySt;
		Integer valueInt;
		IntWritable value;
		try {
			reader = new SequenceFile.Reader(c, SequenceFile.Reader.file(new Path(sequencePath)));

			key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), c);
			value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), c);
			
			while (reader.next(key, value)) {
				keySt = key.toString().trim();
//				valueInt = Integer.parseInt(value.toString());
//				System.out.println("Contains... "+keySt);
				if(!itemsets.contains(keySt)){
					if((valueInt = newItemsets.get(keySt)) == null){
						newItemsets.put(keySt,value.get());
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void checkNewItemsets(Configuration c){
		String[] lineSpt;
		Set<Entry<String,Integer>> keysValues = newItemsets.entrySet();
		List<Entry<String,Integer>> itemsetsToSave = new ArrayList<Entry<String,Integer>>();
		for(Entry<String,Integer> entry: keysValues){
//			System.out.println("sup "+entry.getValue()+" tr "+Main.totalTransactionCount+" sr "+Main.supportRate);
			if(((double)entry.getValue())/((double)Main.totalTransactionCount) >= Main.supportRate){
				//Partial itemset is frequent
				lineSpt = entry.getKey().trim().split(" ");
//				if(realItemsets[lineSpt.length - 1] == null){
//					realItemsets[lineSpt.length - 1] = new ArrayList<String>();
//				}
//				realItemsets[lineSpt.length - 1].add(entry.getKey().trim()+","+entry.getValue());
				if (itemsetsCounts[lineSpt.length - 1] == null) {
					itemsetsCounts[lineSpt.length - 1] = new Integer(1);
				} else {
					itemsetsCounts[lineSpt.length - 1]++;
				}
				
				itemsetsToSave.add(entry);
			}
		}
		
		System.out.println("\n******\tNew itemsets to save\t******\n");
		System.out.println(itemsetsToSave.size());
		
		MrUtils.saveEntryArrayInHdfs(c, itemsetsToSave);
	}
	
	public static String countItemsets() {
		itemsetsCounts = new Integer[20];
		realItemsets = new ArrayList[20];
		itemsets = new HashSet<String>();
		newItemsets = new HashMap<String,Integer>();
		List<String> partitions = MrUtils.getPartitions(Main.outputPartialNameDir);
		ArrayList<String> outputFiles = MrUtils.getAllOuputFilesNames(Main.user + "output1");
		Configuration c = new Configuration();
		c.set("fs.defaultFS", "hdfs://master/");
		for (String outFile : outputFiles) {
			System.out.println("Contando itemsets em " + outFile);
			CountItemsets.countByOutputDir(c,outFile);
		}
		outputFiles = MrUtils.getAllOuputFilesNames(Main.user + "output2");
		for (String outFile : outputFiles) {
			System.out.println("Contando itemsets em " + outFile);
			CountItemsets.countByOutputDir(c,outFile);
		}
		//for partitions
		for (String seqFile : partitions) {
			System.out.println("Contando itemsets em " + seqFile);
			countBySequenceDir(c, Main.outputPartialNameDir+seqFile);
		}
		checkNewItemsets(c);
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
				Collections.sort(realItemsets[i],ITEMSET_ORDER);
				System.out.println("\n********** "+(i+1)+" = "+itemsetsCounts[i]);
				for(String item: realItemsets[i]){
					System.out.println("****** "+item);
				}
			}
		}
	}
	
	private static void addItemset(String item){
		String[] itemArray = item.split("\\s+");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < itemArray.length-1; i++){
			sb.append(itemArray[i]).append(" ");
		}
//		System.out.println("Adding... "+sb.toString().trim());
		itemsets.add(sb.toString().trim());
	}
	
	private static Comparator<String> ITEMSET_ORDER = new Comparator<String>(){

		@Override
		public int compare(String o1, String o2) {
			String[] item1 = o1.split("\\s+");
			String[] item2 = o2.split("\\s+");
			
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
