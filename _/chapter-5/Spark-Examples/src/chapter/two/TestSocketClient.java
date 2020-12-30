package chapter.two;

import java.io.*;
import java.net.*;

public class TestSocketClient {

	public static void main(String[] args) {
		try{
			//Creating
			Socket soc = new Socket("127.0.0.1",9000);
			InputStream inputStream = soc.getInputStream();
			BufferedReader read = new BufferedReader(new InputStreamReader(inputStream));
								
			while(true){
				System.out.println("Waiting for the data from server");
				String data = read.readLine();
				System.out.println("Socket workign fine - Here is data recived - "+data);
			}
			
		}catch(Exception e ){
			e.printStackTrace();
		}


	}

}
