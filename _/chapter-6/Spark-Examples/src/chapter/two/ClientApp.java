package chapter.two;

import java.net.ServerSocket;
import java.net.Socket;
import java.io.*;

public class ClientApp {

	public static void main(String[] args) {
		try{
			System.out.println("Defining new Socket");
			ServerSocket soc = new ServerSocket(9087);
			System.out.println("Waiting for Incoming Connection");
			Socket clientSocket = soc.accept();

			System.out.println("Connection Received");
			OutputStream outputStream = clientSocket.getOutputStream();
			//Keep Reading the data in a Infinite loop and send it over to the Socket.		
			while(true){
				PrintWriter out =  new PrintWriter(outputStream, true);
				BufferedReader read = new BufferedReader(new InputStreamReader(System.in));
				System.out.println("Waiting for user to input some data");
				String data = read.readLine();
				System.out.println("Data received and now writing it to Socket");
				out.println(data);
				
			}
			
		}catch(Exception e ){
			e.printStackTrace();
		}


	}

}
