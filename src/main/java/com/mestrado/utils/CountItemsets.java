package main.java.com.mestrado.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import main.java.com.mestrado.main.MainSpark;

public class CountItemsets {
	private static Integer itemCounts[];
	private static HashSet<String> itemsets;
	private static HashMap<String,Integer> newItemsets;

	private static void countByOutputDir(Configuration c, String outputPath) {
		Path path = new Path(outputPath);

		try {
			FileSystem fs = FileSystem.get(c);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line;
			String[] lineSpt;
			while ((line = br.readLine()) != null) {
				line = line.replaceAll("\\(||\\)", "").replaceAll(",.*", "");
				itemsets.add(line.trim());
				lineSpt = line.split(" ");
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

	private static void countBySequenceDir(Configuration c, String sequencePath) {
		SequenceFile.Reader reader;
		Text key;
		String keySt;
		Integer valueInt;
		Text value;
		String[] lineSpt;
		try {
			reader = new SequenceFile.Reader(c, SequenceFile.Reader.file(new Path(sequencePath)));

			key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), c);
			value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), c);
			
			while (reader.next(key, value)) {
				keySt = key.toString().replaceAll(":.*", "").trim();
//				valueInt = Integer.parseInt(value.toString());
				
				if(!itemsets.contains(keySt)){
					if((valueInt = newItemsets.get(keySt)) == null){
						
						valueInt = Integer.parseInt(value.toString());
						newItemsets.put(keySt,valueInt);
					}else{
						valueInt += Integer.parseInt(value.toString());
						newItemsets.put(keySt,valueInt);
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
			if(((double)entry.getValue())/((double)MainSpark.totalTransactionCount) >= MainSpark.supportRate){
				//Partial itemset is frequent
				lineSpt = entry.getKey().trim().split(" ");
				if (itemCounts[lineSpt.length - 1] == null) {
					itemCounts[lineSpt.length - 1] = new Integer(1);
				} else {
					itemCounts[lineSpt.length - 1]++;
				}
				
				itemsetsToSave.add(entry);
			}
		}
		
		System.out.println("\n******\tNew itemsets to save\t******\n");
		System.out.println(itemsetsToSave.size());
		
		SparkUtils.saveEntryArrayInHdfs(c, itemsetsToSave);
	}

	public static String countItemsets(List<String> filesNames) {
		itemCounts = new Integer[20];
		itemsets = new HashSet<String>();
		newItemsets = new HashMap<String,Integer>();
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", MainSpark.clusterUrl);

		for (String dirName : filesNames) {
			if (!dirName.contains("partition")) {
				List<String> outputFiles = SparkUtils.getAllFilesInDir(conf, dirName);
				for (String outFile : outputFiles) {
					System.out.println("Contando itemsets em " + outFile);
					CountItemsets.countByOutputDir(conf, outFile);
				}
			}
		}

		for (String dirName : filesNames) {
			if (dirName.contains("partition")) {
				// read as sequence file
				List<String> sequenceFiles = SparkUtils.getAllFilesInDir(conf, dirName);
				for (String seqFile : sequenceFiles) {
					System.out.println("Contando itemsets em " + seqFile);
					countBySequenceDir(conf, seqFile);
				}
			}
		}
		checkNewItemsets(conf);
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
		return sb.toString();
	}
}