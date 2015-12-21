package main.java.com.mestrado.utils;

import java.io.Serializable;
import java.util.Comparator;

public class ComparatorUtilsOne implements Comparator<String>, Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(String o1, String o2) {
		return Integer.valueOf(o1).compareTo(Integer.valueOf(o2));
	}
}
