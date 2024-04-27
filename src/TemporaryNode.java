import java.io.*;
import java.net.*;
import java.nio.Buffer;
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
    private Writer writer;
    private BufferedReader reader;

    public boolean start(String startingNodeName, String startingNodeAddress) {
        try {
            // name of tempnode
            this.name = "happytempnode@city.ac.uk:FirstNewTempNodeTest,1.0";

            System.out.println("Address passed to TempNode: " + startingNodeAddress);

            // connecting to startingnode
            // splitting the address into two through the colon
            // part before colon is the IP address
            // part after is the port number
            String[] parts = startingNodeAddress.split(":");
            String ipAddress = parts[0];
            int portNumber = Integer.parseInt(parts[1]);
            InetAddress host = InetAddress.getByName(ipAddress);

            System.out.println("IP Address: " + ipAddress + ", port number: " + portNumber + ". SUCCESS");

            // initializing values for socket (socket object), out by reading output stream and in reading input stream
            socket = new Socket(host, portNumber);
            System.out.println(" TEMP Socket created");
            writer = new OutputStreamWriter(socket.getOutputStream());
            System.out.println("TEMP Output stream..");
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            System.out.println("TEMP Input stream..");

            System.out.println("Is socket connected? " + socket.isConnected());

            // START message
            clientWrite("START 1 " + this.name);
            System.out.println("did you see the start message?");

            // wait until receives "START" response
            String response = reader.readLine();
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
            // split key & value into lines
            String[] keyLines = key.split("\n");
            String[] valueLines = value.split("\n");

            // send first line of PUT request
            clientWrite("PUT? " + keyLines.length + " " + valueLines.length + "\n" + key + value);


            // wait for the response
            String response = reader.readLine();
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

            // Split the key into lines
            String[] keyLines = key.split("\n");

            // Send the GET? request
            clientWrite("GET? " + keyLines.length + "\n" + key);

//            for (String line : keyLines) {
//                clientWrite(line);
//            }

            // Wait for the response
            String response = reader.readLine();

            // If the response starts with "VALUE", read the value
            if (response != null && response.startsWith("VALUE")) {
                String[] parts = response.split(" ");
                int valueLines = Integer.parseInt(parts[1]);
                StringBuilder valueBuilder = new StringBuilder();
                for (int i = 0; i < valueLines; i++) {
                    valueBuilder.append(reader.readLine()).append("\n");
                }
                return valueBuilder.toString().trim();
            } else if (response != null && response.equals("NOPE")) {
                // If the response is "NOPE", the key was not found
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private boolean clientWrite(String message) {
        try {
            writer.write(message + '\n');
            System.out.println(name + ": " + message);
            writer.flush();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private String readInput() {
        Scanner input = new Scanner(System.in);
        String inp = input.nextLine();
        return inp;
    }

    public static void main(String[] args) {
        TemporaryNode tn = new TemporaryNode();
        if(tn.start("happytempnode:FirstNewTempNodeTest,1.0", "127.0.0.1:20000")){
            tn.store("hello\n", "world\n");
            tn.get("hello\n");
        }
    }
}
