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
import java.util.Map.Entry;
import java.util.Set;

import org.apache.spark.api.java.function.Function2;

import main.java.com.mestrado.app.HashNode;
import main.java.com.mestrado.app.HashPrefixTree;
import scala.Tuple2;

/**
 * Count partial itemsets.
 * 
 * @author eduardo
 */
public class Map2Spark2 implements Function2<Integer, Iterator<String>, Iterator<Tuple2<String, Integer>>> {

	private static final long serialVersionUID = 1L;
	private List<Tuple2<String, Integer>> partition;
	private HashMap<String, Integer> itemSup;
	private HashPrefixTree hpt;
	private int maxK;
	private List<Tuple2<String, Integer>> keyValue;

	public Map2Spark2(List<Tuple2<String, Integer>> partition) {
		this.partition = partition;
	}

	@Override
	public Iterator<Tuple2<String, Integer>> call(Integer v1, Iterator<String> v2) throws Exception {
//		System.out.println("In map for partition " + v1 + " and partitions names are...");
		itemSup = new HashMap<String, Integer>();
		hpt = new HashPrefixTree();
		maxK = 0;
		boolean checkPartition = false;
		keyValue = new ArrayList<Tuple2<String, Integer>>();
		keyValue.add(new Tuple2<String, Integer>("#", 0));
		String[] pName;
		for (Tuple2<String, Integer> t : partition) {
			pName = t._1.split(":");
			for(int i = 1; i < pName.length; i++){
				if (String.valueOf(v1).equalsIgnoreCase(pName[i])) {
//				System.out.println(t._1);
//				System.out.println("Build hash tree for this partition, t._2 have spark partition:\n" + t._2);
					if(i == 1) keyValue.add(new Tuple2<String,Integer>(pName[0],t._2));
					buildHashTree(pName[0], 0);
					checkPartition = true;
				}
			}
		}

		partition.clear();
		if (checkPartition)	{
			String[] itemset;
			String[] tr;
			int itemsetIndex;
			while (v2.hasNext()) {
				tr = v2.next().trim().split(" ");
				for (int i = 0; i < tr.length; i++) {
					itemset = new String[maxK];
					itemsetIndex = 0;
					subSet(tr, hpt.getHashNode(), i, itemset, itemsetIndex);
				}
			}
			Set<Entry<String,Integer>> itemsSup = itemSup.entrySet();
			Tuple2<String,Integer> tuple;
			for(Entry<String,Integer> entry: itemsSup){
				tuple = new Tuple2<String,Integer>(entry.getKey(),entry.getValue());
				keyValue.add(tuple);
			}
		}
		// return keyValue == null || keyValue.isEmpty() ? null :
		// keyValue.iterator();
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
		Integer value;
		if ((value = itemSup.get(item)) != null) {
			value++;
			itemSup.put(item, value);
//			keyValue.add(new Tuple2<String, String>(item, value[0] + ":1"));
		}
	}

	private void buildHashTree(String itemset, Integer supPartial) {
		String[] keySpl = itemset.split(" ");
		itemSup.put(itemset, supPartial);
		hpt.add(hpt.getHashNode(), keySpl, 0);
		if (keySpl.length > maxK) {
			maxK = keySpl.length;
		}
	}
}