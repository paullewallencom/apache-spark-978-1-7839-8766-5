package chapter.six;

import java.io.FileWriter;

import org.codehaus.jettison.json.JSONObject;
import org.json.simple.JSONArray;

public class CreateJSON {
	
	public void createComppanyJSON(){
		try{
			JSONObject data_A = new JSONObject();
			data_A.put("Name", "DEPT_A");
			data_A.put("No_Of_Emp", 10);
			data_A.put("No_Of_Supervisors", 2);
			
			JSONObject data_B = new JSONObject();
			data_B.put("Name", "DEPT_B");
			data_B.put("No_Of_Emp", 12);
			data_B.put("No_Of_Supervisors", 2);
			
			JSONObject data_C = new JSONObject();
			data_C.put("Name", "DEPT_C");
			data_C.put("No_Of_Emp", 14);
			data_C.put("No_Of_Supervisors", 3);
			
			JSONObject data_D = new JSONObject();
			data_D.put("Name", "DEPT_D");
			data_D.put("No_Of_Emp", 10);
			data_D.put("No_Of_Supervisors", 1);
			
			JSONObject data_E = new JSONObject();
			data_E.put("Name", "DEPT_E");
			data_E.put("No_Of_Emp", 20);
			data_E.put("No_Of_Supervisors", 5);
			
		JSONObject companyXDept = new JSONObject();
		
		JSONArray departments = new JSONArray();
		departments.add(data_A);
		departments.add(data_B);
		departments.add(data_C);
		departments.add(data_D);
		departments.add(data_E);
		
		String path = "c://Temp//company.json";
		
		FileWriter file = new FileWriter(path);
		file.write(departments.toString());
		file.flush();
		file.close();
		
		}catch(Exception e ){
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		
		new CreateJSON().createComppanyJSON();
			}

}
