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
interface FullNodeInterface {
    public boolean listen(String ipAddress, int portNumber);
    public void handleIncomingConnections(String startingNodeName, String startingNodeAddress);
}
// DO NOT EDIT ends


public class FullNode implements FullNodeInterface {

    private String name;
    private String address;
    private Map<String, String> networkMap;

    public boolean listen(String ipAddress, int portNumber) {
        try {
            // Set the address of the full node
            this.address = ipAddress + ":" + portNumber;

            // Start listening for incoming connections
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
            // Set the name of the full node
            this.name = "Soliman.Majam@city.ac.uk:FirstNewFullNodeTest,1.0";

            // Connect to the starting node and notify other full nodes of its address
            String[] parts = startingNodeAddress.split(":");
            String ipAddress = parts[0];
            int portNumber = Integer.parseInt(parts[1]);
            Socket socket = new Socket(ipAddress, portNumber);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // Send START message
            out.println("START 1 " + this.name);

            // Wait for response
            String response = in.readLine();
            if (response != null && response.startsWith("START")) {
                // Send NOTIFY request
                out.println("NOTIFY");
                out.println(this.name);
                out.println(this.address);

                // Wait for response
                response = in.readLine();
                if (response != null && response.equals("NOTIFIED")) {
                    // Initialize the network map
                    this.networkMap = new HashMap<>();
                    this.networkMap.put(startingNodeName, startingNodeAddress);
                }
            }

            // Close the connection
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleConnection(Socket clientSocket) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

            // Process incoming requests
            String request;
            while ((request = in.readLine()) != null) {
                if (request.startsWith("PUT?")) {
                    // Handle PUT request
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

                    // Store the (key, value) pair
                    // For simplicity, assume it's stored successfully
                    out.println("SUCCESS");

                } else if (request.startsWith("GET?")) {
                    // Handle GET request
                    String[] parts = request.split(" ");
                    int keyLines = Integer.parseInt(parts[1]);
                    StringBuilder keyBuilder = new StringBuilder();
                    for (int i = 0; i < keyLines; i++) {
                        keyBuilder.append(in.readLine()).append("\n");
                    }
                    String key = keyBuilder.toString();

                    // Check if the key exists in the network map
                    // For simplicity, assume it's found and return a dummy value
                    out.println("VALUE 1");
                    out.println("Dummy Value\n");

                } else if (request.equals("NEAREST?")) {
                    // Handle NEAREST request
                    // For simplicity, assume it returns a dummy response
                    out.println("NODES 1");
                    out.println("Dummy Node Name");
                    out.println("Dummy Node Address");

                } else if (request.equals("ECHO?")) {
                    // Handle ECHO request
                    out.println("OHCE");

                } else {
                    // Invalid request
                    out.println("END Invalid request");
                    break;
                }
            }

            // Close the connection
            clientSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
