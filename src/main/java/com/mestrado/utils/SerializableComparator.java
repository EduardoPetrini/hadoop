package main.java.com.mestrado.utils;

import java.util.Comparator;

import scala.Serializable;

public class SerializableComparator implements Comparator<String>, Serializable {

	@Override
	public int compare(String o1, String o2) {
		String[] items1 = o1.split(" ");
		String[] items2 = o2.split(" ");
		
		for (int i = 0;i < items1.length; i++) {
			if (Integer.parseInt(items1[i]) < Integer.parseInt(items2[i])) {
				return -1;
			} else if (Integer.parseInt(items1[i]) > Integer.parseInt(items2[i])) {
				return 1;
			}
		}
		
		return 0;
	
	}
}
