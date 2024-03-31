import java.io.*;
import java.net.*;
import java.util.*;

// IN2011 Computer Networks
// Coursework 2023/2024
//
// Submission by
// Soliman Majam
// 220003523
// Soliman.Majam@city.ac.uk


// DO NOT EDIT starts
interface TemporaryNodeInterface {
    public boolean start(String startingNodeName, String startingNodeAddress);
    public boolean store(String key, String value);
    public String get(String key);
}
// DO NOT EDIT ends


public class TemporaryNode implements TemporaryNodeInterface {

    private String name;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    public boolean start(String startingNodeName, String startingNodeAddress) {
        try {
            // name of tempnode
            this.name = "happytempnode:FirstNewTempNodeTest,1.0 + '\n'";

            System.out.println("Address passed to TempNode: " + startingNodeAddress);

            // connecting to startingnode
            // splitting the address into two through the colon
            // part before colon is the IP address
            // part after is the port number
            String[] parts = startingNodeAddress.split(":");
            String ipAddress = parts[0];
            int portNumber = Integer.parseInt(parts[1]);

            System.out.println("IP Address: " + ipAddress + ", port number: " + portNumber + ". SUCCESS");

            // initializing values for socket (socket object), out by reading output stream and in reading input stream
            this.socket = new Socket(ipAddress, portNumber);
            System.out.println("Socket created");
            this.out = new PrintWriter(socket.getOutputStream(), true);
            System.out.println("Output stream..");
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            System.out.println("Input stream..");
            System.out.println("SUCCESS");

            System.out.println(socket.isConnected());

            // START message
            out.print("START 1 " + this.name + '\n');
            System.out.println("did you see the start message?");

            // wait until receives "START" response
            String response = in.readLine();
            if (response != null && response.startsWith("START")) {
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean store(String key, String value) {
        try {
            // send PUT with key and value
            out.println("PUT? 1 1");
            out.println(key);
            out.println(value);

            // wait until receives "SUCCESS" response
            String response = in.readLine();
            if (response != null && response.equals("SUCCESS")) {
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public String get(String key) {
        try {
            // send GET with key
            out.println("GET? 1");
            out.println(key);

            // wait until receives "VALUE" or "NOPE" response
            String response = in.readLine();
            // if response is "VALUE" the 'key' number of lines are read and recorded then returned
            if (response != null && response.startsWith("VALUE")) {
                int numberOfLines = Integer.parseInt(response.split(" ")[1]);
                StringBuilder valueBuilder = new StringBuilder();
                for (int i = 0; i < numberOfLines; i++) {
                    valueBuilder.append(in.readLine()).append("\n");
                }
                return valueBuilder.toString();
                // if response is "NOPE" nothing happens
            } else if (response != null && response.equals("NOPE")) {
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
