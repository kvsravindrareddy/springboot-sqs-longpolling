package com.veera.data;

import java.io.Serializable;

public class Employee implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5045649201055985223L;
	private String id;
	private String name;
	private String sal;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getSal() {
		return sal;
	}
	public void setSal(String sal) {
		this.sal = sal;
	}
	@Override
	public String toString() {
		return "Employee [id=" + id + ", name=" + name + ", sal=" + sal + "]";
	}
	
	
	
}