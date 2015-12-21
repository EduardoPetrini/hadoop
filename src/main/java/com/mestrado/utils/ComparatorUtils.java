package main.java.com.mestrado.utils;

import java.io.Serializable;
import java.util.Comparator;

public class ComparatorUtils implements Comparator<String>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(String obj1, String obj2) {
		String[] o1 = obj1.split(" ");
		String[] o2 = obj2.split(" ");
		for(int i =0; i < o1.length; i++){
			if(Integer.parseInt(o1[i]) < Integer.parseInt(o2[i])){
				return -1;
			}else if(Integer.parseInt(o1[i]) > Integer.parseInt(o2[i])){
				return 1;
			}
		}
		return 0;
	}

}