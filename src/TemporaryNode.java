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
    Writer writer;
    BufferedReader reader;

    public boolean start(String startingNodeName, String startingNodeAddress) {
        try {
            // name of tempnode
            this.name = "happytempnode:FirstNewTempNodeTest,1.0";

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
            this.socket = new Socket(host, portNumber);
            System.out.println(" TEMP Socket created");
            this.writer = new OutputStreamWriter(socket.getOutputStream());
            System.out.println("TEMP Output stream..");
            this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            System.out.println("TEMP Input stream..");
            System.out.println("SUCCESS");

            System.out.println(socket.isConnected());

            // START message
            clientWrite(writer,"START 1 " + this.name); //  IT GOES UP TO HERE
            System.out.println("did you see the start message?");
            writer.flush();

            // wait until receives "START" response
            String response = reader.readLine();
            /*String startParts[] = response.split(" ");
            String startPart1 = startParts[0];
            String startPart2 = startParts[1];
            String startPart3 = startParts[2];*/
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
            System.out.println("enter put request: ");
            String putInput = readInput();
            String putParts[] = putInput.split(" ");
            String keyNumberS = putParts[1];
            String valNumberS = putParts[2];

            clientWrite(writer, "PUT? " + keyNumberS + " " + valNumberS);
            clientWrite(writer, key);
            clientWrite(writer, value);


            // wait until receives "SUCCESS" response
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
            // send GET with key
            clientWrite(writer, "GET? 1");
            clientWrite(writer, key);

            // wait until receives "VALUE" or "NOPE" response
            String response = reader.readLine();
            // if response is "VALUE" the 'key' number of lines are read and recorded then returned
            if (response != null && response.startsWith("VALUE")) {
                int numberOfLines = Integer.parseInt(response.split(" ")[1]);
                StringBuilder valueBuilder = new StringBuilder();
                for (int i = 0; i < numberOfLines; i++) {
                    valueBuilder.append(reader.readLine()).append("\n");
                }
                writer.flush();
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

    private boolean clientWrite(Writer writer, String message) {

        try {
            writer.write(message + '\n');
            System.out.println(name + ": " + message);
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
        if(tn.start("SolimanMajam@city.ac.uk", "172.27.16.1:2345")){
            tn.store("hello\n", "world\n");
        }
    }
}
