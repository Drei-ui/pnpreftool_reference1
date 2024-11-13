import java.util.Scanner;
import java.io.*;
import java.util.Properties;

public class pnpRefTool {
    public static void main(String[] args) {
        // Load configuration
        Properties config = loadConfig("config/config.properties");
        if (config == null) {
            System.err.println("Failed to load configuration. Exiting...");
            return;
        }

        Scanner scanner = new Scanner(System.in);

        //Prompt for handling type
        System.out.print("Choose handling type (general/special): ");
        String handlingType = scanner.nextLine().toLowerCase().trim();

        while (!handlingType.equals("general") && !handlingType.equals("special")) {
            System.out.println("Invalid handling type. Please enter 'general' or 'special'");
            System.out.print("Choose handling type (general/special): ");
            handlingType = scanner.nextLine().toLowerCase().trim();
        }

        // Call appropriate handler
        if (handlingType.equals("special")) {
            System.out.println("Using special handling...");
            specialHandler.main(args);
        } else {
            System.out.println("Using general handling...");
            generalHandler.main(args);
        }

        scanner.close();
    }

    private static Properties loadConfig(String filePath) {
        Properties properties = new Properties();
        try (FileInputStream input = new FileInputStream(filePath)) {
            properties.load(input);
            return properties;
        } catch (IOException e) {
            System.err.println("Error loading configuration: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}
