package com.chinacloud.incetl;

import org.nutz.dao.Dao;
import org.nutz.dao.impl.NutDao;
import org.nutz.dao.impl.SimpleDataSource;

/**
 * Dao工厂，用于获取操作数据库的Dao，适用于Nutz框架
 * @author nivalsoul
 *
 */
public class DaoFactory {
	static SimpleDataSource ds = new SimpleDataSource();
	private static Dao dao = null;
	
	public static void init(String driverClass, String url, String user,String pass) {
		try {
			ds.setDriverClassName(driverClass);
			ds.setJdbcUrl(url);
			ds.setUsername(user);
			ds.setPassword(pass);
			dao = new NutDao(ds);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static Dao getDao() throws Exception {
		if(dao == null){
			throw new Exception("还没有初始数据库连接信息！");
		}
		return dao;
	}
	
	/**
	 * 重置dao
	 */
	public static void reset() {
		dao=null;
	}
	
	/**
	 * /关闭池内所有连接
	 */
	public static void closeAllConnection() {
		ds.close(); 
	}
	
	private DaoFactory() {
		// can not use this constructor
	}
}
