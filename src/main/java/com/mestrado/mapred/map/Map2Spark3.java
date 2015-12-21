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

import org.apache.spark.api.java.function.FlatMapFunction;

import main.java.com.mestrado.app.HashNode;
import main.java.com.mestrado.app.HashPrefixTree;
import scala.Tuple2;

/**
 * Count itemsets by String List K >= 2.
 * 
 * @author eduardo
 */
public class Map2Spark3 implements FlatMapFunction<Iterator<String>, Tuple2<String, Integer>> {

	private static final long serialVersionUID = 1L;
	private List<String[]> itemsetsSpt;
	private HashMap<String, Integer> itemSup;
	private HashPrefixTree hpt;
	private List<Tuple2<String, Integer>> keyValue;
	private int k;

	public Map2Spark3(List<String[]> itemsetsSpt, int k) {
		this.itemsetsSpt = itemsetsSpt;
		this.k = k;
	}

	@Override
	public Iterable<Tuple2<String, Integer>> call(Iterator<String> v2) throws Exception {
		itemSup = new HashMap<String, Integer>();
		hpt = new HashPrefixTree();
		keyValue = new ArrayList<Tuple2<String, Integer>>();
		for (String[] item : itemsetsSpt) {
			if(item.length == k)
				buildHashTree(item);
		}
		itemsetsSpt.clear();

		String[] itemset;
		String[] tr;
		int itemsetIndex;
		while (v2.hasNext()) {
			tr = v2.next().trim().split(" ");
			for (int i = 0; i < tr.length; i++) {
				itemset = new String[k];
				itemsetIndex = 0;
				subSet(tr, hpt.getHashNode(), i, itemset, itemsetIndex);
			}
		}
		Set<Entry<String, Integer>> itemsSup = itemSup.entrySet();
		Tuple2<String, Integer> tuple;
		for (Entry<String, Integer> entry : itemsSup) {
			tuple = new Tuple2<String, Integer>(entry.getKey(), entry.getValue());
			keyValue.add(tuple);
		}
		return keyValue;
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
			if (hNode.getLevel() == k - 1) {
				StringBuilder sb = new StringBuilder();
				for (String item : itemset) {
					if (item != null) {
						sb.append(item).append(" ");
					}
				}
				addToHashItemSupAndSendToReduce(sb.toString().trim());
				itemset[itemsetIndex] = "";
				return;
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
		} else {
			itemSup.put(item, 1);
		}
	}

	private void buildHashTree(String[] itemset) {
		hpt.add(hpt.getHashNode(), itemset, 0);
	}
}