package io.ymq.example.elasticsearch.trs;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.eprobiti.trs.TRSConnection;
import com.eprobiti.trs.TRSException;
import com.eprobiti.trs.TRSRecord;
import com.eprobiti.trs.TRSResultSet;

/**
 * TRS数据库操作类，
 * 可以获取普通检索的连接，或是语义检索的实例
 * @author lq
 *
 */
public class TRSOperation {
	
	private static Properties _properties = null;
	private String strSortMethod = "RELEVANCE";
	private String strStat;
	private String strDefautCols;
	private int iOption = 2;
	private int iHitPointType = 115;
	private boolean bContinue = false;
	private String strSynonymous;
	
	/**
	 * 获取配置文件信息
	 * @param key：属性名
	 * @return 属性值
	 */
	public static String getProperty(String key) {
		if (_properties == null) {
			try {
				InputStream ins = TRSOperation.class
						.getResourceAsStream("/ckmconfig.properties");
				_properties = new Properties();
				_properties.load(ins);
			} catch (Exception ex) {
				_properties = null;
			}
		}

		return _properties.getProperty(key);
	}
	
	/**
	 * 获取trs普通检索的一个连接
	 * @return trs连接
	 * @throws TRSException
	 */
	public TRSConnection getConnect() {
		TRSConnection trsConnection = null;
		try {
			trsConnection = new TRSConnection();
			trsConnection.connect(getProperty("TRSHost"),
					getProperty("TRSPort"), getProperty("TRSUserName"),
					getProperty("TRSPasswd"));
		} catch (TRSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return trsConnection;
	}
	
	public static void main(String[] args) throws Exception  {
		String[] columns = ("SYSID,申请号,申请日,公开（公告）号,公开（公告）日").split(",");
		String[] columns2 = ("权利要求书,说明书").split(",");
		TRSOperation trsOperation = new TRSOperation();
		List<Map<String, String>> list = trsOperation.queryForList("FMZL,FMSQ", "公开（公告）日>='2017.01.01'",
				columns , columns2,0,2);
		for (Map<String, String> map : list) {
			Iterator<Entry<String, String>> iterator = map.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<String, String> entry = iterator.next();
				System.out.println("[" + entry.getKey() + ":" + entry.getValue() + "]");
			}
			System.out.println("=====================================================================");
		}
	}
	
	public List<Map<String, String>> queryForList(String strSources, String strWhere, 
			String[] columns, String[] columns2, long startIndex, long endIndex) throws Exception {	
		
		List<Map<String, String>> resultList = new ArrayList<Map<String, String>>();
		InputStream xml_is = null;
		InputStreamReader isr = null;
		TRSConnection trsConnection = getConnect();		
		TRSResultSet trsresultset = trsConnection.executeSelect(strSources, strWhere,
				strSortMethod, strStat, strDefautCols, iOption, iHitPointType,
				bContinue);
		
		long totalCount = trsresultset.getRecordCount();
		if (totalCount == 0) {
			return resultList;
		} else {
			// 第一次检索，用户不知道总记录数，因此结束下标有可能大于总记录数
			if (endIndex > totalCount) {
				endIndex = totalCount;
			}			
			trsresultset.moveTo(0, startIndex);			
			// 取出本次需要的全部记录
			for (long i = startIndex; i < endIndex; i++) {
				trsresultset.moveTo(0, i);
				Map<String, String> map = new HashMap<String, String>();
				for (int j = 0; j < columns.length; j++) {
					map.put(columns[j], trsresultset.getString(columns[j]));
				}
				for (int j = 0; j < columns2.length; j++) {
					if ("".equals(columns2[j].trim())) {
						continue;
					}
					xml_is = trsresultset.getBinaryStream(columns2[j], 0);
					if (xml_is != null) {
						StringBuffer sb = new StringBuffer();
						isr = new InputStreamReader(xml_is, "UTF-8");
						char buf[] = new char[1024];
						int nBufLen = isr.read(buf);
						while (nBufLen != -1) {
							sb.append(new String(buf, 0, nBufLen));
							nBufLen = isr.read(buf);
						}
						if (sb != null) {
							map.put(columns2[j], sb.toString());
						}
					}
				}
				//设置当前这条专利的数据库名称
				TRSRecord rec = trsresultset.getRecord();
				map.put("patent_db", rec.strName.toUpperCase());
				resultList.add(map);
			}
		}
		trsresultset.close();
		trsConnection.close();
		return resultList;
	}
	
}
