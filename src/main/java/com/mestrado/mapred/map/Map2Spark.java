/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.spark.api.java.function.Function2;

import main.java.com.mestrado.app.HashNode;
import main.java.com.mestrado.app.HashPrefixTree;
import main.java.com.mestrado.main.MainSpark;
import main.java.com.mestrado.utils.SparkUtils;
import scala.Tuple2;

/**
 * Gerar itemsets de tamanho 2.
 * 
 * @author eduardo
 */
public class Map2Spark implements Function2<Integer, Iterator<String>, Iterator<Tuple2<String, String>>> {

	private static final long serialVersionUID = 1L;
	private HashMap<String, Integer[]> itemSup;
	private HashPrefixTree hpt;
	private int maxK;
	private List<Tuple2<String, String>> keyValue;
	private List<String> partitionsDirs;

	public Map2Spark(List<String> partitionsDirs) {
		this.partitionsDirs = partitionsDirs;
	}

	@Override
	public Iterator<Tuple2<String, String>> call(Integer v1, Iterator<String> v2) throws Exception {
		System.out.println("In map for partition " + v1 + " and partitions names are...");
		itemSup = new HashMap<String, Integer[]>();
		hpt = new HashPrefixTree();
		maxK = 0;
		keyValue = new ArrayList<Tuple2<String, String>>();
		keyValue.add(new Tuple2<String,String>("#","#"));
		
//		for (Tuple2<String, String> t : partition[v1-1]) {
//			if (t._1.contains("partition" + v1 + "/")) {
//				System.out.println(t._1);
//				System.out.println("Build hash tree for this partition, t._2 have spark partition:\n" + t._2);
//				// Build hash tree for this partition, t._2 have spark partition step 1 for this partition
//				buildHashTree(t._2);
//				checkPartition = true;
//			}
//		}
		
		System.out.println("Check if partition will execute...");
		if(checkExecutionMap(v1)){
			String[] itemset;
			String[] tr;
			int itemsetIndex;
			String tmp;
			System.out.println("\n____________________________________ block content\n");	
			while (v2.hasNext()) {
				tmp = v2.next();
				System.out.println(tmp);
				tr = tmp.trim().split(" ");
				for (int i = 0; i < tr.length; i++) {
					itemset = new String[maxK];
					itemsetIndex = 0;
					subSet(tr, hpt.getHashNode(), i, itemset, itemsetIndex);
				}
			}
			hpt = null;
		}
		return keyValue.iterator();
	}

	private boolean checkExecutionMap(Integer v1) {
		String[] sp;
		for(String p: partitionsDirs){
			sp = p.split("/");
			
			if(sp[sp.length-1].equalsIgnoreCase("partition"+v1)){
				System.out.println("\n**********************\n\nok-----partition "+v1+" will execute!!!\n**************************\n\n");
				buildHashTree(p);
				return true;
			}
		}
		return false;
	}

	private void subSet(String[] transaction, HashNode hNode, int i, String[] itemset, int itemsetIndex) {

		if (i >= transaction.length) {
			return;
		}

		HashNode son = hNode.getHashNode().get(transaction[i]);

		if (son == null) {
			return;
		} else {
			itemset[itemsetIndex] = transaction[i];

			StringBuilder sb = new StringBuilder();
			for (String item : itemset) {
				if (item != null) {
					sb.append(item).append(" ");
				}
			}
			addToHashItemSupAndSendToReduce(sb.toString().trim());

			i++;
			itemsetIndex++;
			while (i < transaction.length) {
				subSet(transaction, son, i, itemset, itemsetIndex);
				for (int j = itemsetIndex; j < itemset.length; j++) {
					itemset[j] = "";
				}
				i++;
			}
		}
	}

	public void addToHashItemSupAndSendToReduce(String item) {
		Integer[] value;
		if ((value = itemSup.get(item)) != null) {
			keyValue.add(new Tuple2<String, String>(item, value[0] + ":1"));
		}
	}

	private void buildHashTree(String sequenceFileName) {
		System.out.println("\n****************************\n\nGet all partitions files names by file \""+sequenceFileName+"\"...\n\n*******************************\n\n");
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", MainSpark.clusterUrl);
		
		List<String> partitionsFiles = SparkUtils.getAllFilesInDir(conf, sequenceFileName);
		
		SequenceFile.Reader reader;
		Text key;
		Text value;
		for(String file : partitionsFiles){
			System.out.println("\n****************************\n\nReading "+file+"\n\n*******************************\n\n");
			try{
			reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(file)));
			
			key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			
			while (reader.next(key, value)) {
				System.out.println("Key value "+key+" -> "+value);
				addInHashTree(key.toString(), value.toString());
			}
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		
		Set<String> keySet = itemSup.keySet();
		System.out.println("\n****************************\n\nKeys in hash:");
		for(String k: keySet){
			System.out.println(k);
		}
		System.out.println("\n\n*******************************\n\n");
	}

	private void addInHashTree(String line, String sup) {
		String[] keySpl;
		Integer vHash[];
		line = line.replaceAll(":.*", "");
		keySpl = line.split(" ");
		vHash = new Integer[2];
		vHash[0] = Integer.parseInt(sup);
		vHash[1] = 0;
		itemSup.put(line, vHash);
		hpt.add(hpt.getHashNode(), keySpl, 0);
		if (keySpl.length > maxK) {
			maxK = keySpl.length;
		}
	}
}