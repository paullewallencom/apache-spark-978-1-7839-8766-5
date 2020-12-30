package chapter.four;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JavaTransformLog {
	
	enum MapKeys{
		IP,client,user,date,method,request,protocol,respCode,size;
	}

	public static Map<MapKeys, String> parseLog(String logLine){
		
		Matcher matcher = PATTERN.matcher(logLine);
		if (!matcher.find()) {
			System.out.println("Cannot parse logline" + logLine);
		}
		return createDataMap(matcher);		

	}
	
	private static Map<MapKeys, String> createDataMap(Matcher m){

		Map<MapKeys, String> dataMap =  new HashMap<MapKeys,String>();
		dataMap.put(MapKeys.IP,m.group(1));
		dataMap.put(MapKeys.client,m.group(2));
		dataMap.put(MapKeys.user,m.group(3));
		dataMap.put(MapKeys.date,m.group(4));
		dataMap.put(MapKeys.method,m.group(5));
		dataMap.put(MapKeys.request,m.group(6));
		dataMap.put(MapKeys.protocol,m.group(7));
		dataMap.put(MapKeys.respCode,m.group(8));
		dataMap.put(MapKeys.size,m.group(9));
		
		return dataMap;
		
	}

	private static final String LOG_ENTRY_PATTERN = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\S+)";
	private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);
	
	public static void main(String[] args) {
		try {

			BufferedReader brRead = new BufferedReader(new FileReader(new File(
					"c:\\access_log")));

			String data;
			while ((data = brRead.readLine()) != null) {
				parseLog(data);
				Thread.sleep(2000);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

}
