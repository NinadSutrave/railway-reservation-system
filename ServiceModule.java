import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.sql.*;

/**
 * Main Class to controll the program flow
 */
public class ServiceModule {
    // Server listens to port
    static int serverPort = 7008;
    // Max no of parallel requests the server can process
    static int numServerCores = 5;

    // ------------ Main----------------------
    public static void main(String[] args) throws IOException {
        // Creating a thread pool
        ExecutorService executorService = Executors.newFixedThreadPool(numServerCores);

        try (// Creating a server socket to listen for clients
                ServerSocket serverSocket = new ServerSocket(serverPort)) {
            Socket socketConnection = null;

            // Always-ON server
            while (true) {
                System.out.println("Listening port : " + serverPort
                        + "\nWaiting for clients...");
                socketConnection = serverSocket.accept(); // Accept a connection from a client
                System.out.println("Accepted client :"
                        + socketConnection.getRemoteSocketAddress().toString()
                        + "\n");
                // Create a runnable task
                Runnable runnableTask = new QueryRunner(socketConnection);
                // Submit task for execution
                executorService.submit(runnableTask);
            }
        }
    }
}

class QueryRunner implements Runnable {
    // Declare socket for client access
    protected Socket socketConnection;

    public QueryRunner(Socket clientSocket) {
        this.socketConnection = clientSocket;
    }

    public void run() {
        try {
            // Reading data from client
            InputStreamReader inputStream = new InputStreamReader(socketConnection
                    .getInputStream());
            BufferedReader bufferedInput = new BufferedReader(inputStream);
            OutputStreamWriter outputStream = new OutputStreamWriter(socketConnection
                    .getOutputStream());
            BufferedWriter bufferedOutput = new BufferedWriter(outputStream);
            PrintWriter printWriter = new PrintWriter(bufferedOutput, true);
            String clientCommand = "";
            // Read client query from the socket endpoint
            clientCommand = bufferedInput.readLine();
            while (!clientCommand.equals("#")) {

                // System.out.println("Recieved data <" + clientCommand + "> from client : "
                // + socketConnection.getRemoteSocketAddress().toString());

                /*******************************************
                 * Your DB code goes here
                 ********************************************/

                try {

                    Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres",
                            "postgres", "test@123");
                    Statement st = conn.createStatement();

                    int length = clientCommand.length();
                    char lastCharacter = clientCommand.charAt(length - 2);
                    if (lastCharacter >= '0' && lastCharacter <= '9') {
                        String parts[] = clientCommand.split(" ", -1);
                        System.out.println(parts.length);

                        String trainNumber = "'" + parts[0] + "', ";
                        String createdOn = "'" + parts[1] + "', ";
                        String acCoaches = parts[2] + ", ";
                        String slCoaches = parts[3];

                        String query = "insert into train VALUES(" + trainNumber + createdOn + acCoaches + slCoaches
                                + ");";

                        System.out.println(query);

                        ResultSet rs = st.executeQuery(query);
                        while (rs.next()) {
                            System.out.println(rs.getString(1));
                            printWriter.println(rs.getString(1));
                        }
                    } else {
                        String extract = clientCommand.substring(0, clientCommand.indexOf(' '));
                        int numberOfPassengers = Integer.parseInt(extract);
                        if (numberOfPassengers > 0) {
                            extract = clientCommand.substring(clientCommand.indexOf(' '));
                            String parts[] = clientCommand.split(" ", -1);

                            for (int i = 1; i < numberOfPassengers; ++i) {
                                parts[i] = "'" + parts[i].substring(0, parts[i].indexOf(',')) + "',";
                            }
                            parts[numberOfPassengers] = "'" + parts[numberOfPassengers] + "'";

                            int totalParts = parts.length;
                            String coachType = "'" + parts[totalParts - 2] + "', ";
                            String startDate = "'" + parts[totalParts - 3] + "', ";
                            String trainNumber = "'" + parts[totalParts - 4] + "', ";

                            String query = "select allotK(" + trainNumber + startDate + numberOfPassengers + ", "
                                    + coachType + "array[";

                            for (int i = 1; i <= numberOfPassengers; ++i) {
                                query = query + parts[i];
                            }

                            query = query + "]);";

                            System.out.println(query);
                            ResultSet rs = st.executeQuery(query);
                            while (rs.next()) {
                                System.out.println(rs.getString(1));
                                printWriter.println(rs.getString(1));
                            }
                        }
                    }
                } catch (SQLException e) {
                    String check = "No results were returned by the query.";
                    if (e.getMessage().equals(check)) {
                        printWriter.println("Successful Executed");
                    } else {
                        printWriter.println(e.getMessage());
                    }
                } catch (Exception e) {
                    System.out.print(e.getMessage());
                }

                // Dummy response send to client
                // Sending data back to the client
                // printWriter.println(responseQuery);
                // Read next client query
                clientCommand = bufferedInput.readLine();
            }
            inputStream.close();
            bufferedInput.close();
            outputStream.close();
            bufferedOutput.close();
            printWriter.close();
            socketConnection.close();
        } catch (IOException e) {
            return;
        }
    }
}

// java -cp .;postgresql-42.5.0.jar ServiceModule.java
