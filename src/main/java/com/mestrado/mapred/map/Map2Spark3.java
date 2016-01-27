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
import main.java.com.mestrado.app.SupPart;
import scala.Tuple2;

/**
 * Count itemsets by String List K >= 2.
 * 
 * @author eduardo
 */
public class Map2Spark3 implements Function2<Integer, Iterator<String>, Iterator<Tuple2<String, SupPart>>> {

	private static final long serialVersionUID = 1L;
	private List<String[]> partition;
	private HashMap<String, Integer> itemSup;
	private HashPrefixTree hpt;
	private int maxK;
	private List<Tuple2<String, SupPart>> keyValue;
	private double support;

	public Map2Spark3(double support, List<String[]> partition) {
		this.partition = partition;
		this.support = support;
	}

	@Override
	public Iterator<Tuple2<String, SupPart>> call(Integer v1, Iterator<String> v2) throws Exception {
		System.out.println("In map for partition " + v1 + " and partitions names are...");
		itemSup = new HashMap<String, Integer>();
		hpt = new HashPrefixTree();
		maxK = 0;
		long countTr = 0;
		for (String[] item : partition) {
			buildHashTree(item);
		}
		partition.clear();
		keyValue = new ArrayList<Tuple2<String, SupPart>>();
		keyValue.add(new Tuple2<String, SupPart>("#", null));

		String[] itemset;
		String[] tr;
		int itemsetIndex;
		while (v2.hasNext()) {
			countTr++;
			tr = v2.next().trim().split(" ");
			for (int i = 0; i < tr.length; i++) {
				itemset = new String[maxK];
				itemsetIndex = 0;
				subSet(tr, hpt.getHashNode(), i, itemset, itemsetIndex);
			}
		}
		Set<Entry<String, Integer>> itemsSup = itemSup.entrySet();
		Tuple2<String, SupPart> tuple;
		for (Entry<String, Integer> entry : itemsSup) {
			if ((((double) entry.getValue()) / ((double) countTr)) >= support) {
				tuple = new Tuple2<String, SupPart>(entry.getKey(), new SupPart(v1, entry.getValue()));
				keyValue.add(tuple);
			}
		}
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
			if (itemsetIndex == maxK-1) {
				StringBuilder sb = new StringBuilder();
				for (String item : itemset) {
					if (item != null) {
						sb.append(item).append(" ");
					}
				}
				addToHashItemSupAndSendToReduce(sb.toString().trim());
			}

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
			// keyValue.add(new Tuple2<String, String>(item, value[0] + ":1"));
		} else {
			itemSup.put(item, 1);
		}
	}

	private void buildHashTree(String[] keySpl) {
		// String[] keySpl = itemset.split(" ");
		// itemSup.put(itemset, 0);
		hpt.add(hpt.getHashNode(), keySpl, 0);
		if (keySpl.length > maxK) {
			maxK = keySpl.length;
		}
	}
}