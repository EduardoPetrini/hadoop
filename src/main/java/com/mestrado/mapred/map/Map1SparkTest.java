
package main.java.com.mestrado.mapred.map;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

/**
 *
 * @author eduardo
 */
public class Map1SparkTest implements Function2<Integer, Iterator<Tuple2<String,String>>, Iterator<String>> {

	private static final long serialVersionUID = 1L;

	@Override
	public Iterator<String> call(Integer v1, Iterator<Tuple2<String, String>> v2) throws Exception {
		StringBuilder sb = new StringBuilder();
		System.out.println("Printando todo mundo...");
		sb.append("Printando todo mundo...").append("\n");
		System.out.println("v1: "+v1);
		sb.append("v1: "+v1).append("\n");
		System.out.println("Nas tuplas...");
		sb.append("Nas tuplas...").append("\n");
		Tuple2<String, String> t;
		for(;v2.hasNext();){
			t = v2.next();
			System.out.println(t._1+" - "+t._2.length());
			sb.append(t._1+" - "+t._2.length()).append("\n");
		}
		sb.append("\n\n");
		ArrayList<String> array = new ArrayList<String>();
		array.add(sb.toString());
		Iterator<String> it = array.iterator();
		return it;
	}
	
}
