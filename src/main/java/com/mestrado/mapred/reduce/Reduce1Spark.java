/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.reduce;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

/**
 *
 * @author eduardo
 */
public class Reduce1Spark implements FlatMapFunction<Tuple2<String,Iterable<String>>, Tuple2<String,String>>{

	private static final long serialVersionUID = 1L;
	private double support;
	private int totalMaps; //M
	private long totalTransactions; //D
	private ArrayList<String> blocksIds; //Partial
    
	public Reduce1Spark(double support, int totalMaps, long totalTransactions, ArrayList<String> blocksIds) {
		this.support = support;
		this.totalMaps = totalMaps;
		this.totalTransactions = totalTransactions;
		this.blocksIds = blocksIds;
		System.out.println("*************************Na função reduce*******************************");
		System.out.println("Support: "+support);
		System.out.println("totalMaps: "+totalMaps);
		System.out.println("totalTransactions: "+totalTransactions);
		System.out.println("blocksIds: "+blocksIds);
	}

	@Override
	public Iterable<Tuple2<String,String>> call(Tuple2<String, Iterable<String>> t) throws Exception {
		Tuple2<String,String> kv;
		Iterator<String> values = t._2.iterator();
		StringBuilder sb = new StringBuilder();
		
		System.out.println(t._1);
		while(values.hasNext()){
			sb.append(" -> ").append(values.next());
		}
		kv = new Tuple2(t._1,sb.toString());
		return null;
	}
}
