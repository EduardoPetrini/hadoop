/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.app;

import java.io.Serializable;
import java.util.HashMap;

/**
 *
 * @author eduardo
 */
public class SupPart implements Serializable{
	private static final long serialVersionUID = 1L;
	private Integer sup;
	private Integer partitionId;
	
	public SupPart(Integer partitionId){
		sup = new Integer(1);
		this.partitionId = partitionId;
	}
	
	public SupPart(Integer v1, Integer value) {
		// TODO Auto-generated constructor stub
		this.partitionId = v1;
		this.sup = value;
	}

	public void increment(){
		sup++;
	}

	@Override
	public boolean equals(Object obj) {
		return sup.intValue() == ((SupPart)obj).sup.intValue() && partitionId.intValue() == ((SupPart)obj).partitionId.intValue();
	}

	@Override
	public String toString() {
		return sup+":"+partitionId;
	}

	public Integer getPartitionId() {
		return partitionId;
	}

	public void setPartitionId(Integer partitionId) {
		this.partitionId = partitionId;
	}

	public Integer getSup() {
		return sup;
	}

	public void setSup(Integer sup) {
		this.sup = sup;
	}
}