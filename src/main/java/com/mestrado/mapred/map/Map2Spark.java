/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.map;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import main.java.com.mestrado.app.HashNode;
import main.java.com.mestrado.app.HashPrefixTree;
import scala.Tuple2;

/**
 * Gerar itemsets de tamanho 2.
 * 
 * @author eduardo
 */
public class Map2Spark implements Function2<Integer, Iterator<String>, Iterator<Tuple2<String, String>>> {

	private static final long serialVersionUID = 1L;
	private List<Tuple2<String, String>> partition;
	private HashMap<String, Integer[]> itemSup;
	private HashPrefixTree hpt;
	private int maxK;
	private List<Tuple2<String, String>> keyValue;

	public Map2Spark(List<Tuple2<String, String>> partition) {
		this.partition = partition;
	}

	@Override
	public Iterator<Tuple2<String, String>> call(Integer v1, Iterator<String> v2) throws Exception {
		System.out.println("In map for partition " + v1 + " and partitions names are...");
		itemSup = new HashMap<String, Integer[]>();
		hpt = new HashPrefixTree();
		maxK = 0;
		boolean checkPartition = false;
		keyValue = new ArrayList<Tuple2<String, String>>();
		keyValue.add(new Tuple2<String,String>("#","#"));
		for (Tuple2<String, String> t : partition) {
			if (t._1.contains("partition" + v1 + "/")) {
				System.out.println(t._1);
				System.out.println("Build hash tree for this partition, t._2 have spark partition:\n" + t._2);
				// Build hash tree for this partition, t._2 have spark partition step 1 for this partition
				buildHashTree(t._2);
				checkPartition = true;
			}
		}

		partition.clear();
		if (checkPartition) {
			String[] itemset;
			String[] tr;
			int itemsetIndex;
			String tmp;
			while (v2.hasNext()) {
				tmp = v2.next();

				tr = tmp.trim().split(" ");
				for (int i = 0; i < tr.length; i++) {
					itemset = new String[maxK];
					itemsetIndex = 0;
					subSet(tr, hpt.getHashNode(), i, itemset, itemsetIndex);
				}
			}

			if (keyValue.isEmpty()) {
				System.out.println("KeyValue is empty :(");
			} else {
				System.out.println("KeyValue isn't empty :), your content:");
				for (Tuple2<String, String> t : keyValue) {
					System.out.println(t._1 + " : " + t._2);
				}
			}
		}
//		return keyValue == null || keyValue.isEmpty() ? null : keyValue.iterator();
		return keyValue.iterator();
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

	private void buildHashTree(String blockContent) {
		int start = 0;
		int end;
		boolean endBlock = false;
		while ((end = blockContent.indexOf("\n",start)) != -1) {
			addInHashTree(blockContent.substring(start, end).replaceAll("\\(|\\)", "").split(","));
			start = end + 1;
			if (start >= blockContent.length()) {
				endBlock = true;
				break;
			}
		}
		if (!endBlock) {
			addInHashTree(blockContent.substring(start).replaceAll("\\(|\\)", "").split(","));
		}
	}

	private void addInHashTree(String[] line) {
		String[] keySpl;
		Integer vHash[];
		line[0] = line[0].replaceAll(":.*", "");
		keySpl = line[0].split(" ");
		vHash = new Integer[2];
		vHash[0] = Integer.parseInt(line[1]);
		vHash[1] = 0;
		itemSup.put(line[0], vHash);
		hpt.add(hpt.getHashNode(), keySpl, 0);
		if (keySpl.length > maxK) {
			maxK = keySpl.length;
		}
	}
}