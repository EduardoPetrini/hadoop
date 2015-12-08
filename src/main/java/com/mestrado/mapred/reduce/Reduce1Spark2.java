/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.reduce;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFunction;

import main.java.com.mestrado.app.SupPart;
import scala.Tuple2;

/**
 * Split itemsets in global and partial
 * 
 * @author eduardo
 */
public class Reduce1Spark2 implements PairFunction<Tuple2<String, Iterable<SupPart>>, String, Integer> {

	private static final long serialVersionUID = 1L;
	private double support;
	private int totalMaps; // M
	private long totalTransactions; // D
	private ArrayList<String> blocksIds; // Partial
//	private SequenceFile.Writer[] writers;
	private Integer[] dis;

	public Reduce1Spark2(double support, int totalMaps, long totalTransactions, ArrayList<String> blocksIds, Integer[] dis) {
		this.support = support;
		this.totalMaps = totalMaps;
		this.totalTransactions = totalTransactions;
		this.blocksIds = blocksIds;
		this.dis = dis;
		// this.writers = writers;
		System.out.println("*************************On the reduce function*******************************");
		System.out.println("Support: " + support);
		System.out.println("totalMaps: " + totalMaps);
		System.out.println("totalTransactions: " + totalTransactions);
		System.out.println("blocksIds: " + blocksIds);
	}

	@Override
	public Tuple2<String, Integer> call(Tuple2<String, Iterable<SupPart>> t) throws Exception {
		Tuple2<String, Integer> kv = null;
		Iterator<SupPart> values = t._2.iterator();
		int partialSupport = 0;
		long numMapsOfX = 0; // Nx
		SupPart supPartValues;
		Double partialGlobalSupport = new Double(0);
		ArrayList<String> diList = new ArrayList<String>();
		while (values.hasNext()) {
			supPartValues = values.next();
			partialSupport += supPartValues.getSup();
			diList.add(String.valueOf(supPartValues.getPartitionId()));
			numMapsOfX++;
		}

		if ((partialSupport / ((double) totalTransactions)) >= support) {
			return new Tuple2<String, Integer>(t._1, partialSupport);
		}else if (numMapsOfX < totalMaps) {
			for (String partitionId : blocksIds) {
				if (!diList.contains(partitionId)) {
					partialGlobalSupport = calcPartialGlobalSupport(dis[Integer.parseInt(partitionId)], numMapsOfX, partialSupport);
					if ((partialGlobalSupport / ((double) totalTransactions)) >= support) {

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
						kv = new Tuple2<String, Integer>(sb.toString(), partialSupport);
					}
				}
			}
		}
		return kv;

	}

	public double calcPartialGlobalSupport(long di, long nx, int partialSuport) {
		return partialSuport + (((support * di) - 1) * (totalMaps - nx));
	}

	public void printInfo(String key, double partialSupport, long numMapsOfX, long di, ArrayList<String> diList, Double partialSupGlobal, double itemSup) {
		System.out.println("\n\nItemset: " + key);
		System.out.println("Suporte parcial: " + partialSupport);
		System.out.println("Suporte parcial global: " + partialSupGlobal);
		System.out.println("Item sup : " + itemSup);
		System.out.println("Suport rate: " + support);
		System.out.println("Número de Maps do item (Nx): " + numMapsOfX);
		System.out.println("Valor de Di: " + di);
		System.out.println("Toal de Maps: " + totalMaps);
		System.out.println("Total de Transações: " + totalTransactions);
		System.out.println("Mínimo suporte global: " + support * totalTransactions);
		System.out.println("Blocks: " + blocksIds.size() + " : " + blocksIds);
		System.out.println("DiList " + diList.size() + " : " + diList + "\n");

	}

	/***
	 * 
	 * @param key
	 * @param value
	 * @param index
	 */
	public void saveInCache(String key, Integer value, int index) {
//		try {
//			writers[index].append(key, value);
//		} catch (IOException ex) {
//			Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
//		}
	}
}
