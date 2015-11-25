
package main.java.com.mestrado.mapred.map;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.spark.api.java.function.Function2;

import main.java.com.mestrado.app.SupPart;
import scala.Tuple2;

/**
 * Find all frequents 1-itemsets 
 * @author eduardo
 */
public class Map1Spark3 implements Function2<Integer, Iterator<String>, Iterator<Tuple2<String,SupPart>>> {

	private static final long serialVersionUID = 1L;
	private HashMap<String,SupPart> map;
	private Integer partitionId;
	private double support;
	private long countTr;
	
	public Map1Spark3(double support) {
		this.support = support;
		countTr = 0;
	}

	private void add(String item){
		SupPart value;
		if((value = map.get(item)) == null){
			value = new SupPart(partitionId);
			map.put(item,value);
		}else{
			value.increment();
			map.put(item,value);
		}
	}
	
	public Iterator<Tuple2<String, SupPart>> call(Integer v1, Iterator<String> v2) throws Exception {
		String[] tr;
		map = new HashMap<String,SupPart>();
		partitionId = v1;
		
		while(v2.hasNext()){
			countTr++;
			tr = v2.next().split(" ");
			for(String item: tr){
				add(item);
			}
		}
		
		List<Tuple2<String,SupPart>> lista = new ArrayList<Tuple2<String,SupPart>>();
		Entry<String,SupPart> entry;
		Tuple2<String,SupPart> tuple;
		Iterator<Entry<String,SupPart>> entryIterator = map.entrySet().iterator();
		while(entryIterator.hasNext()){
			entry = entryIterator.next();
			if((((double) entry.getValue().getSup())/((double)countTr)) >= support){
				tuple = new Tuple2<String,SupPart>(entry.getKey(),entry.getValue());
				lista.add(tuple);
			}
		}
		
		return lista.iterator();
	}
}
