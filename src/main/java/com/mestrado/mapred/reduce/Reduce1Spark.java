/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 *
 * @author eduardo
 */
public class Reduce1Spark implements PairFunction<Tuple2<String, Iterable<String>>, Text, Text> {

	private static final long serialVersionUID = 1L;
	private double support;
	private int totalMaps; // M
	private long totalTransactions; // D
	private ArrayList<String> blocksIds; // Partial
	private SequenceFile.Writer[] writers;
	private Integer[] dis;

	public Reduce1Spark(double support, int totalMaps, long totalTransactions, ArrayList<String> blocksIds, Integer[] dis) {
		this.support = support;
		this.totalMaps = totalMaps;
		this.totalTransactions = totalTransactions;
		this.blocksIds = blocksIds;
		this.dis = dis;
		// this.writers = writers;
		System.out.println("*************************Na função reduce*******************************");
		System.out.println("Support: " + support);
		System.out.println("totalMaps: " + totalMaps);
		System.out.println("totalTransactions: " + totalTransactions);
		System.out.println("blocksIds: " + blocksIds);
	}

	@Override
	public Tuple2<Text, Text> call(Tuple2<String, Iterable<String>> t) throws Exception {
		Tuple2<Text, Text> kv = null;
		Iterator<String> values = t._2.iterator();
		int partialSupport = 0;
		long numMapsOfX = 0; // Nx
		String[] splitValues;
		Double partialGlobalSupport = new Double(0);
		ArrayList<String> diList = new ArrayList<String>();
//		long di = 0;
		while (values.hasNext()) {
			splitValues = values.next().toString().split(":");
			partialSupport += Integer.valueOf(splitValues[0]);
			diList.add(splitValues[1]);
//			di += Integer.parseInt(splitValues[2]);
			numMapsOfX++;
		}

		if ((partialSupport / ((double) totalTransactions)) >= support) {
			return new Tuple2<Text, Text>(new Text(t._1), new Text(String.valueOf(partialSupport)));
		}else if (numMapsOfX < totalMaps) {
			for (String partitionId : blocksIds) {
				if (!diList.contains(partitionId)) {
					partialGlobalSupport = calcPartialGlobalSupport(dis[Integer.parseInt(partitionId)], numMapsOfX, partialSupport);
					if ((partialGlobalSupport / ((double) totalTransactions)) >= support) {

						// Item parcialmente frequente, enviá-lo para partições
						// em que não foi frequente]
						boolean cameFromThePartition;
						StringBuilder sb = new StringBuilder();
						sb.append(t._1);
						for_ext: for (int i = 0; i < blocksIds.size(); i++) {
							cameFromThePartition = false;
							for (int j = 0; j < diList.size(); j++) {
								if (blocksIds.get(i).equalsIgnoreCase(diList.get(j))) {
									cameFromThePartition = true;
									continue for_ext;
								}
							}
							if (!cameFromThePartition) {
								// saveInCache(t._1+":"+i, partialSupport, i);
								sb.append(":").append(i);
							}
						}
						kv = new Tuple2<Text, Text>(new Text(sb.toString()), new Text(String.valueOf(partialSupport)));
					}
				}
			}
			// di = totalTransactions/totalMaps;
			// if(t._1.equalsIgnoreCase("3 5")){
			// System.out.println("At 3 and 5 partial support
			// "+partialGlobalSupport+" so...\n
			// "+(partialGlobalSupport/((double) totalTransactions))+" >=
			// "+support+" ?");
			// printInfo(t._1, partialSupport, numMapsOfX, di,diList);
			// }
		}
		return kv;
	}

	public double calcPartialGlobalSupport(long di, long nx, int partialSuport) {
		return partialSuport + (((support * di) - 1) * (totalMaps - nx));
	}

	public void printInfo(String key, double partialSupport, long numMapsOfX, long di, ArrayList<String> diList) {
		System.out.println("Itemset: " + key);
		System.out.println("Suporte parcial: " + partialSupport);
		System.out.println("Suport rate: " + support);
		System.out.println("Número de Maps do item (Nx): " + numMapsOfX);
		System.out.println("Valor de Di: " + di);
		System.out.println("Toal de Maps: " + totalMaps);
		System.out.println("Total de Transações: " + totalTransactions);
		System.out.println("Mínimo suporte global: " + support * totalTransactions);
		System.out.println("Blocks: " + blocksIds.size() + " : " + blocksIds);
		System.out.println("DiList " + diList.size() + " : " + diList);

	}

	/***
	 * 
	 * @param key
	 * @param value
	 * @param index
	 */
	public void saveInCache(String key, Integer value, int index) {
		try {
			writers[index].append(key, value);
		} catch (IOException ex) {
			Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}
