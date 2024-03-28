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

    public boolean listen(String ipAddress, int portNumber) {
        try {
            // set node address
            this.address = ipAddress + ":" + portNumber;

            // listen for any connections with new ServerSocket
            ServerSocket serverSocket = new ServerSocket(portNumber);
            new Thread(() -> {
                try {
                    while (true) {
                        Socket clientSocket = serverSocket.accept();
                        handleConnection(clientSocket);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();

            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
    
    public void handleIncomingConnections(String startingNodeName, String startingNodeAddress) {
        try {
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
            Socket socket = new Socket(ipAddress, portNumber);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            socket.connect(); // CONNECT IT NOW REMEMMBER

            // START message
            out.println("START 1 " + this.name);

            // wait until receives 'START' response
            String response = in.readLine();
            if (response != null && response.startsWith("START")) {
                // sned 'NOTIFY' request
                out.println("NOTIFY");
                out.println(this.name);
                out.println(this.address);

                // wait until receives 'NOTIFIED' response
                response = in.readLine();
                if (response != null && response.equals("NOTIFIED")) {
                    // if response successful, initialize that network hash map and add the connected node to it
                    this.networkMap = new HashMap<>();
                    this.networkMap.put(startingNodeName, startingNodeAddress);
                }
            }

            // end connection
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleConnection(Socket clientSocket) {
        try {
            // recognise requests and responses
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

            // different request possibilities
            String request;
            while ((request = in.readLine()) != null) {
                // if 'PUT?' request then
                if (request.startsWith("PUT?")) {
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
                    byte[] valueHashID;
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
                    } else {
                        // "FAILED" as this node is not one of three closest nodes
                        out.println("FAILED");
                    }

                } else if (request.startsWith("GET?")) {
                    // if 'GET?' request then read lines
                    String[] parts = request.split(" ");
                    int keyLines = Integer.parseInt(parts[1]);
                    StringBuilder keyBuilder = new StringBuilder();
                    for (int i = 0; i < keyLines; i++) {
                        keyBuilder.append(in.readLine()).append("\n");
                    }
                    // save read key as String
                    String key = keyBuilder.toString().trim(); // trim to remove newline

                    // check if key exists in network map
                    if (networkMap.containsKey(key)) {
                        // if true get key's value
                        String value = networkMap.get(key);

                        // split value into lines to count number of lines
                        String[] valueLines = value.split("\n");

                        // send 'VALUE' and the number of lines
                        out.println("VALUE " + valueLines.length);
                        out.print(value); // then the actual value
                        out.println(); // + newline
                    } else {
                        // if false send 'NOPE'
                        out.println("NOPE");
                    }
                } else if (request.equals("NEAREST?")) {
                    // Handle NEAREST request
                    // For simplicity, assume it returns a dummy response
                    out.println("NODES 1");
                    out.println("Dummy Node Name");
                    out.println("Dummy Node Address");

                } else if (request.equals("ECHO?")) {
                    // send reverse
                    out.println("OHCE");

                } else if (request.equals("END")) {
                    out.println("END");
                    clientSocket.close();
                    break;

                } else {
                    // invalid request
                    out.println("END");
                    clientSocket.close();
                    break;
                }
            }

            // close the connection
            clientSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isClosestNode(byte[] valueHashID) {
        // get the distances between valueHashID and hashIDs of nodes in the network map
        List<Integer> distances = new ArrayList<>();
        for (String nodeHashID : networkMap.keySet()) {
            byte[] nodeHashIDBytes = hexStringToByteArray(nodeHashID); // String hashID to byte array conversion
            // method calculates distance between two bytes, adds the distance calculated to distances
            distances.add(calculateDistance(nodeHashIDBytes, valueHashID));
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
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

    private int calculateDistance(byte[] hashID1, byte[] hashID2) {
        int distance = 0;
        for (int i = 0; i < hashID1.length; i++) {
            // bitwise XOR of corresponding bytes (turns it into a series of 1s and 0s)
            int xorResult = hashID1[i] ^ hashID2[i];

            // count leading zeroes to work out which leading bits are matching
            int leadingZeros = Integer.numberOfLeadingZeros(xorResult);

            // computer distance based on formula we got (256 - matching leading bits)
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
        byte[] valueHashID = HashID.computeHashID(value); // calculates hash value of value

        // start for loop iterating over the values... hashing them.. then commparing distance.. for every value
        for (String v : networkMap.values()) {
            String existingValue = networkMap.get(v);
            byte[] existingValueHashID = HashID.computeHashID(existingValue);

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
}
