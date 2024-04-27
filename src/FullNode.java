import java.io.*;
import java.net.*;
import java.sql.SQLOutput;
import java.util.*;

// IN2011 Computer Networks
// Coursework 2023/2024
//
// Submission by
// Soliman Majam
// 220003523
// Soliman.Majam@city.ac.uk


// DO NOT EDIT starts
interface FullNodeInterface {
    public boolean listen(String ipAddress, int portNumber);
    public void handleIncomingConnections(String startingNodeName, String startingNodeAddress);
}
// DO NOT EDIT ends


public class FullNode implements FullNodeInterface {

    // comprised of email and unique string
    private String name;
    //comprised of IP address and port number
    private String address;
    //hash map for storing other nodes' address
    private Map<String, String> networkMap;

    private Writer writer;
    private BufferedReader in;

    public boolean listen(String ipAddress, int portNumber) {
        try {
            // set node address
            this.address = ipAddress + ":" + portNumber;

            // listen for any connections with new ServerSocket
            ServerSocket serverSocket = new ServerSocket(portNumber);
            System.out.println("listened successfully");
            serverSocket.close();


            return true;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void handleIncomingConnections(String startingNodeName, String startingNodeAddress) {
        try {
            System.out.println("Handling connection..");
            // name of node
            this.name = "Soliman.Majam@city.ac.uk:FirstNewFullNodeTest,1.0";

            // connect to node nad let other nodes know
            // address made up of ip address and port number
            String[] parts = startingNodeAddress.split(":");
            String ipAddress = parts[0];

            if (parts.length != 2) {
                System.err.println("Invalid startingNodeAddress format: " + startingNodeAddress);
                return;
            }

            int portNumber = Integer.parseInt(parts[1]);
            ServerSocket socket = new ServerSocket();
            socket.bind(new InetSocketAddress(ipAddress, portNumber));
            System.out.println("Server socket created");
            Socket clientSocket = socket.accept();
            System.out.println("Client socket created");
            writer = new OutputStreamWriter(clientSocket.getOutputStream());
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            // START message
            serverWrite(writer,"START 1 " + this.name);

            // wait until receives 'START' response
            String response = in.readLine();
            if (response != null && response.startsWith("START ")) {
                System.out.println("Message accepted");
                // sned 'NOTIFY' request
                serverWrite(writer, "NOTIFY");
                serverWrite(writer, this.name);
                serverWrite(writer, this.address);

                // wait until receives 'NOTIFIED' response
                response = in.readLine();
                if (response != null && response.equals("NOTIFIED")) {
                    // if response successful, initialize that network hash map and add the connected node to it
                    this.networkMap = new HashMap<>();
                    this.networkMap.put(startingNodeName, startingNodeAddress);

                    handleRequests(clientSocket);
                }
                System.out.println("Connection established, ready for incoming requests: " + '\n');
                handleRequests(clientSocket);
            }

            // end connection
            clientSocket.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleRequests(Socket clientSocket) {
        try {

            writer = new OutputStreamWriter(clientSocket.getOutputStream());
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            StringBuilder requestBuilder = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                requestBuilder.append(line).append("\n");

                // Check if the request is complete
                String request = requestBuilder.toString().trim();

                // if 'PUT?' request then
                if (request.startsWith("PUT? ")) {
                    // read values and keys
                    String[] parts = request.split(" ");
                    int keyLines = Integer.parseInt(parts[1]);
                    int valueLines = Integer.parseInt(parts[2]);
                    StringBuilder keyBuilder = new StringBuilder();
                    StringBuilder valueBuilder = new StringBuilder();
                    for (int i = 0; i < keyLines; i++) {
                        keyBuilder.append(in.readLine()).append("\n");
                    }
                    for (int i = 0; i < valueLines; i++) {
                        valueBuilder.append(in.readLine()).append("\n");
                    }
                    String key = keyBuilder.toString();
                    String value = valueBuilder.toString();

                    // Compute the hashID for the value to be stored
                    String valueHashID;
                    try {
                        valueHashID = HashID.computeHashID(value);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    // check networkMap for if valueHashID is one of the closest nodes
                    boolean isClosest = isClosestNode(valueHashID);

                    if (isClosest) {
                        // check if 3 nodes don't already have the same distance
                        addToMap(key, value);
                        serverWrite(writer, "SUCCESS");

                    } else {
                        // "FAILED" as this node is not one of three closest nodes
                        serverWrite(writer, "FAILED");
                    }

                } else if (request.startsWith("GET? ")) {
                    System.out.println(request);
                    String[] parts = request.split(" ");
                    int keyLines = Integer.parseInt(parts[1]);

                    StringBuilder keyBuilder = new StringBuilder();
                    String[] requestLines = requestBuilder.toString().split("\n");
                    for (int i = 1; i <= keyLines; i++) {
                        keyBuilder.append(requestLines[i]).append("\n");
                    }
                    String key = keyBuilder.toString().trim(); // trim to remove newline

                    // check if key exists in network map
                    if (networkMap.containsKey(key)) {
                        // if true get key's value
                        String value = networkMap.get(key);

                        // split value into lines to count number of lines
                        String[] valueLines = value.split("\n");

                        // send 'VALUE' and the number of lines
                        serverWrite(writer, "VALUE " + valueLines.length);
                        serverWrite(writer, value); // then the actual value
                    } else {
                        // if false send 'NOPE'
                        serverWrite(writer, "NOPE");
                    }
                } else if (request.equals("NEAREST?")) {
                    // handle NEAREST request
                    // for now assume it returns a dummy response
                    serverWrite(writer,"NODES 1");
                    serverWrite(writer,"Dummy Node Name");
                    serverWrite(writer,"Dummy Node Address");

                } else if (request.equals("ECHO?")) {
                    // send reverse
                    serverWrite(writer,"OHCE");

                } else if (request.equals("END")) {
                    serverWrite(writer,"END");
                    //clientSocket.close();
                    return;

                } else {
                    // invalid request
                    serverWrite(writer, "INVALID");
                    System.out.println("Invalid Request: " + request);
                    serverWrite(writer,"END");
                    //clientSocket.close();
                    return;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isClosestNode(String valueHashID) {
        // get the distances between valueHashID and hashIDs of nodes in the network map
        List<Integer> distances = new ArrayList<>();
        for (String nodeHashID : networkMap.keySet()) {
            // method calculates distance between two strings, adds the distance calculated to distances
            distances.add(calculateDistance(nodeHashID, valueHashID));
        }

        // sort distances in ascending order
        Collections.sort(distances);

        // check for closest three nodes
        int count = 0;
        for (int distance : distances) {
            if (distance == 0) {
                // skip
                continue;
            }
            if (count >= 3) {
                // done
                break;
            }
            if (distance <= getCurrentNodeDistanceThreshold()) {
                // node that is close enough has been found
                count++;
            }
        }

        // if count is less than 3, current node is one of three closest nodes
        return count < 3;
    }

    // hex String to byte array conversion method
    private byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    private int calculateDistance(String hashID1, String hashID2) {
        int distance = 0;
        for (int i = 0; i < hashID1.length(); i++) {
            // bitwise XOR of hex chars
            int xorResult = Character.digit(hashID1.charAt(i), 16) ^ Character.digit(hashID2.charAt(i), 16);

            // count leading matching 0's
            int leadingZeros = Integer.numberOfLeadingZeros(xorResult);

            // compute distance based on formula (256 - matching leading bits)
            distance += 256 - leadingZeros;
        }
        return distance;
    }

    private int getCurrentNodeDistanceThreshold() {
        // i don't knwo what the threshold would be
        return 100;
    }

    public void addToMap(String key, String value) throws Exception {
        int count = 0; // keeps track of amount of nodes with same distance
        String valueHashID = HashID.computeHashID(value); // calculates hash value of value

        // start for loop iterating over the values... hashing them.. then commparing distance.. for every value
        for (Map.Entry<String, String> entry : networkMap.entrySet()) {
            String existingValue = entry.getValue();
            String existingValueHashID = HashID.computeHashID(existingValue);

            // calculate distance between HashID of current value and new value
            int distance = calculateDistance(existingValueHashID, valueHashID);

            // if distance is 0 means they're the same, increment count
            if (distance == 0) {
                count++;

                // if count == then 3 nodes have the same distance already, exit method without adding the key, value pair
                if (count >= 3) {
                    System.out.println("Three nodes with same distance already present");
                    System.out.println("FAILED");
                    return;
                }
            }
        }

        // if all pairs have been checked and count is less than 3, add the new key, value pair
        networkMap.put(key, value);
        System.out.println("SUCCESS");
    }

    private boolean serverWrite(Writer writer, String message) {

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
}
