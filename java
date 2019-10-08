package com.wyzw.api.utils;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Model;
import com.jfinal.plugin.activerecord.Page;
import com.jfinal.plugin.activerecord.Record;
import com.jfinal.plugin.activerecord.SqlPara;
import com.wyzw.api.common.cachekey.CacheRegisterEnum;
import com.wyzw.api.common.cachekey.CacheRegistrationDiscoveryCenter;

public class DbCache {
	
		public static final int TIMEOUT_LEVEL_SECOND = 10;
		public static final int TIMEOUT_LEVEL_MINUTE = 60;
		public static final int TIMEOUT_LEVEL_HOUR = 60 * 60;
		public static final int TIMEOUT_LEVEL_DAY = 60 * 60 * 24;
		public static final int TIMEOUT_LEVEL_WEEK = 60 * 60 * 24 * 7;
		public static final int TIMEOUT_LEVEL_MONTH = 60 * 60 * 24 * 7 *30 ;
		
		public static final int TIMEOUT_LEVEL_VERYSHORT = 10;
		public static final int TIMEOUT_LEVEL_SHORT = 60;
		public static final int TIMEOUT_LEVEL_NOMAL = 60 * 10;
		public static final int TIMEOUT_LEVEL_MEDIUM = 60 * 60;
		public static final int TIMEOUT_LEVEL_LONG = 60 * 60 * 24;
		public static final int TIMEOUT_LEVEL_VERYLONG = 60 * 60 * 24 * 7;
	   /* ------------------ 针对SQL进行结果的缓存(redis)---------------------------- */
    
		static final int DEFAULT_TIME_FOR_CACHED_FOR_SQL = 600; //变化不太频繁的数据，时间不变
		static final String CACHE_PREFIX_FOR_SQL = "CachedForSql_";
		

		
	    /**
	     * 为一个sql语句得到缓存（redis)，如果没有，则从数据库中查询。它只适合于时间不敏感的Sql
	     * @author zcl
	     * @param sql
	     * @param params
	     * @return
	     */
	    @SuppressWarnings("unchecked")
		public static List<Record> find(String sql,Object...params) {
	    	int seconds = DEFAULT_TIME_FOR_CACHED_FOR_SQL;
	    	return find(seconds, sql, params);
	    }
	    
	    public static Record findFirst(String sql, Object...params) {
	    	List<Record> list = find(sql,params);
	    	if (list == null || list.size() == 0) {
	    		return null;
	    	}
	    	return find(sql,params).get(0);
	    }
	    /**
	     * 为一个sql语句得到缓存（redis)，如果没有，则从数据库中查询。它只适合于时间不敏感的Sql
	     * 同时把这个sqlkey 注册到缓存注册发现中心
	     * @author zcl
	     * @param sql
	     * @param params
	     * @return
	     */
	    @SuppressWarnings("unchecked")
		public static List<Record> findAndRegister(CacheRegisterEnum cacheRegistInfo, String sql,Object...params) {
	    	int seconds = DEFAULT_TIME_FOR_CACHED_FOR_SQL;
	    	return findAndRegister(cacheRegistInfo, seconds, sql, params);
	    }
	    
	    /*@SuppressWarnings("unchecked")
		public static List<Record> findAndRegister(CacheRegisterEnum cacheRegistInfo, int seconds, String sql, Object...params) {
	    	String key = createSqlKey(sql, params);
	    	// 向缓存注册发布中心注册key
	    	CacheRegistrationDiscoveryCenter.register(cacheRegistInfo.getProjectName(), cacheRegistInfo.getOperation(), key);
	    	
	    	Object object = CacheUtil.get(key);
	    	if(object == null) {
	    		object = Db.find(sql, params);
	    		if(object==null) return null;
	    		CacheUtil.set(key, object);    		
	    		CacheUtil.setTimeOut(key, seconds);
	    	}
	    	return (List<Record>)object;
	    }*/
	    
	    @SuppressWarnings("unchecked")
		public static List<Record> findAndRegister(CacheRegisterEnum cacheRegistInfo, int seconds, String sql, Object...params) {
	    	return findAndRegister(null, cacheRegistInfo,   seconds,  sql, params) ;
	    }
	    
	    @SuppressWarnings("unchecked")
		public static List<Record> findAndRegister(Object objectId, CacheRegisterEnum cacheRegistInfo, int seconds, String sql, Object...params) {
	    	String key = createSqlKey(sql, params);
	    	// 向缓存注册发布中心注册key
	    	String realOperation = cacheRegistInfo.getOperation()+(objectId==null?"":"_"+objectId);
	    	CacheRegistrationDiscoveryCenter.register(cacheRegistInfo.getProjectName(), realOperation, key);
	    	
	    	Object object = CacheUtil.get(key);
	    	if(object == null) {
	    		object = Db.find(sql, params);
	    		if(object==null) return null;
	    		CacheUtil.set(key, object);    		
	    		CacheUtil.setTimeOut(key, seconds);
	    	}
	    	return (List<Record>)object;
	    }
	    
	    /**
	     * 过期注册发现中心的key
	     * @author zcl
	     * @param cacheRegistInfo
	     * @return
	     */
	    public static void expireCacheRegisterKey(CacheRegisterEnum cacheRegistInfo) {
	    	expireCacheRegisterKey(null, cacheRegistInfo);
	    }
	    
	    /**
	     * 过期注册发现中心的key
	     * @author zcl
	     * @param cacheRegistInfo
	     * @param objectId
	     * @return
	     */
	    public static void expireCacheRegisterKey(Object objectId, CacheRegisterEnum cacheRegistInfo) {
	    	String realOperation = cacheRegistInfo.getOperation()+(objectId==null?"":"_"+objectId);
	    	String realKey = CacheRegistrationDiscoveryCenter.find(cacheRegistInfo.getProjectName(), realOperation);
	    	CacheUtil.remove(realKey);
	    }
	    
	    @SuppressWarnings("unchecked")
		public static List<Record> find(int seconds, String sql, Object...params) {
	    	String key = createSqlKey(sql, params);
	    	Object object = CacheUtil.get(key);
	    	if(object == null) {
	    		object = Db.find(sql, params);
	    		if(object==null) return null;
	    		CacheUtil.set(key, object);    		
	    		CacheUtil.setTimeOut(key, seconds);
	    	}
	    	return (List<Record>)object;
	    }
	    
	    @SuppressWarnings("unchecked")
  		public static List<Record> find(int seconds, String sql) {
  	    	String key = createSqlKey(sql,null);
  	    	Object object = CacheUtil.get(key);
  	    	if(object == null) {
  	    		object = Db.find(sql);
  	    		if(object==null) return null;
  	    		CacheUtil.set(key, object);    		
  	    		CacheUtil.setTimeOut(key, seconds);
  	    	}
  	    	return (List<Record>)object;
  	    }
	    
	    public static int queryInt(String sql,Object...params) throws Exception {
	    	Record record = findFirst(sql, params);
	    	if (record == null) {
	    		throw new Exception("没有查询到记录");
	    	}
	    	
	    	String[] columnNames = record.getColumnNames();
	    	if (columnNames == null || columnNames.length == 0) {
	    		throw new Exception("没有查询到记录");
	    	}
	    	
	    	return record.getInt(columnNames[0]);
	    }
	    
	    public static int queryInt(int seconds,String sql,Object...params) throws Exception {
	    	Record record = findFirst(seconds,sql, params);
	    	if (record == null) {
	    		throw new Exception("没有查询到记录");
	    	}
	    	
	    	String[] columnNames = record.getColumnNames();
	    	if (columnNames == null || columnNames.length == 0) {
	    		throw new Exception("没有查询到记录");
	    	}
	    	
	    	return record.get(columnNames[0]) == null ? 0 : record.getInt(columnNames[0]);
	    }
	    
	    private static String createSqlKey(String sql, Object[] params) {
	    	StringBuilder sqlKey = new StringBuilder();
	    	sqlKey.append(CACHE_PREFIX_FOR_SQL).append(sql);
	    	if (params != null && params.length > 0) {
	    		for (int i = 0; i < params.length; i++) {
		    		sqlKey.append(params[i].toString()).append("@");
				}
	    	}
			return sqlKey.toString();
		}

		@SuppressWarnings("unchecked")
		public static Record findFirst(int seconds, String sql, Object...params) {
			String key = createSqlKey(sql, params);
	    	Object object = CacheUtil.get(key);
	    	if(object == null) {
	    		object = Db.findFirst(sql, params);
	    		if(object==null) return null;
	    		CacheUtil.set(key, object);    		
	    		CacheUtil.setTimeOut(key, seconds);
	    	}
	    	return (Record)object;
	    }
	    
	    public static void setForSql(int seconds, String sql, Object...params) {
	    	String key = CACHE_PREFIX_FOR_SQL  + sql;
	    	List<Record>records = Db.find(sql, params);
			//if(object==null) return null;
			CacheUtil.set(key, records);    		   	
	    }
	    
	    
	    public static Page<Record> paginate(int seconds, int pageNumber, int pageSize,SqlPara sqlPara){
	    	String key = createSqlKey(sqlPara.getSql() + "_" + pageNumber + "_" + pageSize,sqlPara.getPara());
  	    	Object object = CacheUtil.get(key);
  	    	if(object == null) {
  	    		object = Db.paginate(pageNumber, pageSize, sqlPara);
  	    		if(object==null) return null;
  	    		CacheUtil.set(key, object);    		
  	    		CacheUtil.setTimeOut(key, seconds);
  	    	}
  	    	return (Page<Record>)object;
	    }
	    
	    public static Page<Record> paginate( int pageNumber, int pageSize,SqlPara sqlPara){
	    	return paginate(DEFAULT_TIME_FOR_CACHED_FOR_SQL,pageNumber, pageSize, sqlPara);
	    }
	    
	    /*--------------------------针对model的缓存--------------------------*/
	    @SuppressWarnings("unchecked")
		public static <M extends Model> List<M> find(int seconds ,M dao , String sql, Object...params) {
	    	String key = createSqlKey(sql, params);
	    	Object object = CacheUtil.get(key);
	    	if(object == null) {
	    		object = dao.find(sql, params);
	    		if(object==null) return null;
	    		CacheUtil.set(key, object);    		
	    		CacheUtil.setTimeOut(key, seconds);
	    	}
	    	return (List<M>)object;
	    }
	    
	    @SuppressWarnings("unchecked")
		public static  <M extends Model> List<Model<M>>  find(M dao , String sql, Object...params) {
	    	return find(DEFAULT_TIME_FOR_CACHED_FOR_SQL, dao, sql, params);
	    }
	  
	    public static <M extends Model> M findFirst(int seconds ,M dao , String sql, Object...params){
	    	List<Model<M>> list = find(seconds, dao, sql, params);
	    	if (list == null || list.size() == 0) {
	    		return null;
	    	}
	    	
	    	return (M) list.get(0);
	    }
	    
	    public static  <M extends Model> M findFirst(M dao , String sql, Object...params) {
	    	return  findFirst(DEFAULT_TIME_FOR_CACHED_FOR_SQL, dao, sql, params);
	    }
	    
	    
	    
	    
	    /* ------------------ 针对SQL进行结果的缓存 (static变量)---------------------------- */
	  
		static final int STATIC_DEFAULT_TIME_FOR_CACHED_FOR_SQL = 600; //变化不太频繁的数据，时间不变
		static final String STATIC_CACHE_PREFIX_FOR_SQL = "StaticCachedForSql_";
		//这个static的Map对象，记录的是一个key对应的两项值（数组中包括：时间及数据）
		private static Map<String, Object[]> mapRecords = new ConcurrentHashMap <>(); //线程安全
		
		/*
		 * 针对SQL进行结果的缓存 (static变量)
		 */
		
	    @SuppressWarnings("unchecked")
		public static List<Record> find_byMemory(String sql,Object...params) throws Exception {
	    	int seconds = STATIC_DEFAULT_TIME_FOR_CACHED_FOR_SQL;
	    	return find_byMemory(seconds, sql, params);
	    }
	    
	    public static List<Record> find_byMemory(int seconds, String sql, Object...params) throws Exception {
	    	String key = createSqlKeyByMemory(sql , params);
	    	List<Record> records = get_byMemory(key);
	    	if(records == null) {
	    		setForSql_byMemory(seconds, sql, params);
	    		return get_byMemory(key);
	    	}
	    	return records;
	    }
	    
	    public static void setForSql_byMemory(int seconds, String sql, Object...params)  throws Exception{
	    	String key = createSqlKeyByMemory(sql , params);
	    	List<Record>records = Db.find(sql, params);
			if(records==null) return;
			
			Object[] objects = {DateUtils.addSeconds(new Date(), seconds), records}; //当时间加上秒数得到过期时间
			mapRecords.put(key, objects);		    		   	
	    }
	    
	    private static List<Record>get_byMemory(String key) throws Exception {
	    	boolean has = mapRecords.containsKey(key);
	    	if(!has)return null;
	    	
	    	Object[] objects = mapRecords.get(key);
	    	if(objects==null || objects.length<2) return null;
	    	
	    	Date expireTime = (Date) objects[0]; //时间
	    	@SuppressWarnings("unchecked")
			List<Record> records = (List<Record>)objects[1];//数据
	    	if( DateUtils.after(new Date(), expireTime)) { //还没过期
	    		return records;
	    	}else {
	    		mapRecords.remove(key); //过期的自动清除掉
	    	}
	    	return null;
	    }
	    
	    private static String createSqlKeyByMemory(String sql, Object[] params) {
	    	StringBuilder sqlKey = new StringBuilder();
	    	sqlKey.append(STATIC_CACHE_PREFIX_FOR_SQL).append(sql);
	    	for (int i = 0; i < params.length; i++) {
	    		sqlKey.append(params[i].toString()).append("@");
			}
			return sqlKey.toString();
		}
	    
}
