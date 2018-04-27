package io.ymq.example.elasticsearch.trs;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.Map.Entry;

import com.alibaba.fastjson.JSONObject;
import com.eprobiti.trs.TRSConnection;
import com.eprobiti.trs.TRSException;
import com.eprobiti.trs.TRSRecord;
import com.eprobiti.trs.TRSResultSet;
import io.ymq.example.elasticsearch.run.Startup;
import io.ymq.example.elasticsearch.utils.ElasticsearchUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * TRS数据库操作类，
 * 可以获取普通检索的连接，或是语义检索的实例
 * @author lq
 *
 */
@SpringBootApplication
@ComponentScan(value = {"io.ymq.example.elasticsearch"})
public class TRSOperationThread  extends Thread{

	private String threadName;
	private Long from;
	private Long to;
	private int batchSize;
	
	private static Properties _properties = null;
	private String strSortMethod = "RELEVANCE";
	private String strStat;
	private String strDefautCols;
	private int iOption = 2;
	private int iHitPointType = 115;
	private boolean bContinue = false;
	private String strSynonymous;

	@Override
	public void run() {

		long batchNum = (to - from + batchSize - 1)/batchSize;
		for (int i = 0; i < batchNum; i++) {
			long startIndex = from + batchSize*i;
			long endIndex = startIndex + batchSize;
			if (endIndex > to) {
				endIndex = to;
			}
			long start = System.currentTimeMillis();

			try {
				//ETL
				trs2es(startIndex, endIndex);
			} catch (Exception e) {
				e.printStackTrace();
			}

			long consume = (System.currentTimeMillis() - start) / 1000;
			System.out.println(new Date() + "["+ threadName + "]" + startIndex + "-" + endIndex + "耗时:" + consume);
		}
	}

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
			e.printStackTrace();
		}
		
		return trsConnection;
	}

	public static void main(String[] args) throws Exception  {
		SpringApplication.run(Startup.class, args);

		int threadNums = 10;
		int first = 0;
		int last = 285 * 10000;//284,9793
//		last = 20000;
		for (int i = 0; i < threadNums; i++) {
			String threadName = "Thread-" + (i+1);
			long from = first + (last - first)/threadNums * i;
			long to = first + (last - first)/threadNums * (i+1);
			System.out.println(threadName + "," + from + "," + to);
			TRSOperationThread thread = new TRSOperationThread(threadName,from,to,1000);
			thread.start();
		}
	}

	private void trs2es(long startIndex, long endIndex) throws Exception {
		long start = System.currentTimeMillis();
		String[] columns = ("SYSID,APP_ID,PUB_ID,申请号,申请日,公开（公告）号,公开（公告）日,专利号,专利类型,REPRESENTATIVE,名称,摘要,"
				+ "说明书附图,分类号,主分类号,欧洲分类号,欧洲主分类号,本国分类号,本国主分类号,申请（专利权）人,发明（设计）人,专利代理机构,代理人,地址,"
				+ "申请国代码,国省代码,省,市,区,申请来源,国际申请,国际公布,进入国家日期,优先权,优先权号,优先权日,同族专利项,参考文献,分案原申请号,审查员,"
				+ "颁证日,页数,发布路径,缩略图发布路径,摘要附图存储路径,公报发布路径,公报所在页,公报翻页信息,关键词,自动摘要,公开年,TABLE_SN,VERSION,"
				+ "主权项,专利权状态,专利权状态代码,法律状态,范畴分类,族号,旧申请号,旧分类号,旧申请（专利权）人,旧发明（设计）人,引证文献,旧优先权,"
				+ "同日申请,PDF地址,复审类型,名称关键词,独权关键词,背景关键词").split(",");
		String[] columns2 = ("权利要求书,说明书").split(",");
		TRSOperation trsOperation = new TRSOperation();
		String where = "公开（公告）日>'2017.10.01'";
		where = "SYSID='%'";
		List<Map<String, String>> list = trsOperation.queryForList("FMSQ", where,
				columns , columns2, startIndex, endIndex);
		int i = 0;
		for (Map<String, String> map : list) {
			ElasticsearchUtils.addData(JSONObject.parseObject(JSONObject.toJSONString(map)),
					"patent", "fmsq", "id=" + map.get("SYSID"));
			i++;
			if(i % 10 == 0){
				long consume = (System.currentTimeMillis() - start) / 1000;
//				System.out.println(new Date() + "[insert]" + i + "耗时:" + consume);
			}
		}
	}

	private static void trstest() throws Exception {
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
	public TRSOperationThread() {
		super();
	}

	public TRSOperationThread(String threadName, Long from, Long to, int batchSize) {
		super();
		this.threadName = threadName;
		this.from = from;
		this.to = to;
		this.batchSize = batchSize;
	}
}
