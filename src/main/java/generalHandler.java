import java.io.*;
import java.sql.*;
import java.util.*;
import java.nio.file.*;
import java.util.stream.Collectors;
import net.ucanaccess.jdbc.UcanaccessDriver;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class generalHandler {
    static {
        try {
            Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
        } catch (ClassNotFoundException e) {
            System.err.println("Error loading UCanAccess driver: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Properties config = loadConfig("config/config.properties");

        String inputPath = config.getProperty("Input_path");
        String schemaMdbPath = config.getProperty("Schema_MDB");
        String outputPath = config.getProperty("Output_path");
        String inputExt = config.getProperty("input_ext", "QCA");

        // Add prompt for output format
        String outputFormat;
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("Choose output format (mdb/csv): ");
            outputFormat = scanner.nextLine().toLowerCase().trim();
            if (outputFormat.equals("mdb") || outputFormat.equals("csv")) {
                break;
            }
            System.out.println("Invalid format. Please enter 'mdb' or 'csv'");
        }
        scanner.close();

        Map<String, String> fieldNumberToName = getFieldNumberToNameMapping(schemaMdbPath);

        File inputDir = new File(inputPath);
        File[] qcaFiles = inputDir.listFiles((dir, name) -> name.toLowerCase().endsWith("." + inputExt.toLowerCase()));

        //create an output directory if non existent
        try{
            Path outputDir = Paths.get(outputPath);
            Files.createDirectories(outputDir);
            System.out.println("Output path at: " + outputDir);
        } catch(Exception e){
            System.err.println("Error creating output directory: " + e.getMessage());
            e.printStackTrace();
        }

        if (qcaFiles != null && qcaFiles.length > 0) {
            List<Map<String, String>> allRecords = new ArrayList<>();
            
            // Get the base name from the first file
            String baseName = qcaFiles[0].getName().split("-")[0] + "_db";
            
            for (File qcaFile : qcaFiles) {
                System.out.println("Processing file: " + qcaFile.getName());
                Map<String, String> fieldValues = extractFieldValuesFromXml(qcaFile, fieldNumberToName);
                if (!fieldValues.isEmpty()) {
                    allRecords.add(fieldValues);
                }
            }

            // Output based on user's choice
            if (outputFormat.equals("csv")) {
                writeToCSV(allRecords, outputPath + "/" + baseName + ".csv");
            } else {
                String outputMdbPath = outputPath + "/" + baseName + ".mdb";
                writeToMDB(allRecords, outputMdbPath);
            }
        }
    }

    private static Properties loadConfig(String filePath) {
        Properties properties = new Properties();
        try (FileInputStream input = new FileInputStream(filePath)) {
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    private static Map<String, String> getFieldNumberToNameMapping(String schemaMdbPath) {
        Map<String, String> fieldNumberToName = new HashMap<>();
        String url = "jdbc:ucanaccess://" + schemaMdbPath;
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            String query = "SELECT FieldNo, FieldName FROM tblFields";
            ResultSet rs = stmt.executeQuery(query);
            
            while (rs.next()) {
                String fieldNo = rs.getString("FieldNo").replace(".0", "").trim();
                String fieldName = rs.getString("FieldName");
                fieldNumberToName.put(fieldNo, fieldName);
            }
        } catch (Exception e) {
            System.err.println("Error in field mapping: " + e.getMessage());
            e.printStackTrace();
        }
        return fieldNumberToName;
    }

    private static Map<String, String> extractFieldValuesFromXml(File qcaFile, Map<String, String> fieldNumberToName) {
        Map<String, String> fieldValues = new HashMap<>();

        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(qcaFile);
            doc.getDocumentElement().normalize();

            NodeList records = doc.getElementsByTagName("record");
            System.out.println("Found " + records.getLength() + " records");

            for (int i = 0; i < records.getLength(); i++) {
                Element record = (Element) records.item(i);
                NodeList subRecords = record.getElementsByTagName("sub-record");
                
                for (int j = 0; j < subRecords.getLength(); j++) {
                    Element subRecord = (Element) subRecords.item(j);
                    NodeList fields = subRecord.getElementsByTagName("field");
                    
                    // Get document type
                    String documentType = "";
                    for (int k = 0; k < fields.getLength(); k++) {
                        Element field = (Element) fields.item(k);
                        if ("1".equals(field.getAttribute("no"))) {
                            documentType = field.getElementsByTagName("value").item(0).getTextContent();
                            break;
                        }
                    }

                    // Process all fields regardless of document type
                    for (int k = 0; k < fields.getLength(); k++) {
                        Element field = (Element) fields.item(k);
                        String fieldNo = field.getAttribute("no");
                        String value = field.getElementsByTagName("value").item(0).getTextContent();
                        
                        if (value.equals("$") || value.trim().isEmpty()) {
                            continue;
                        }

                        // Try both with and without document type
                        String fieldName = fieldNumberToName.get(fieldNo + "-" + documentType);
                        if (fieldName == null) {
                            fieldName = fieldNumberToName.get(fieldNo);
                        }

                        if (fieldName != null) {
                            fieldValues.put(fieldName, value);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fieldValues;
    }

    private static String sanitizeFieldName(String fieldName) {
        return fieldName.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    public static void writeToMDB(List<Map<String, String>> records, String outputMdbPath) {
        if (records.isEmpty()) return;

        outputMdbPath = outputMdbPath.replace('/', '\\');
        
        try {
            Files.deleteIfExists(Paths.get(outputMdbPath));
            System.out.println("Deleted existing database file (if any)");
        } catch (IOException e) {
            System.err.println("Warning: Could not delete existing file: " + e.getMessage());
        }

        String url = "jdbc:ucanaccess://" + outputMdbPath + ";newdatabaseversion=V2000";
        
        try (Connection conn = DriverManager.getConnection(url)) {
            Map<String, String> fieldMapping = new HashMap<>();
            for (Map<String, String> record : records) {
                for (String field : record.keySet()) {
                    String normalizedField = field.toUpperCase();
                    if (!fieldMapping.containsKey(normalizedField)) {
                        fieldMapping.put(normalizedField, field);
                    }
                }
            }
            
            StringBuilder createTableSQL = new StringBuilder();
            createTableSQL.append("CREATE TABLE dataBase (ID COUNTER PRIMARY KEY");
            for (String originalField : fieldMapping.values()) {
                String sanitizedField = sanitizeFieldName(originalField);
                createTableSQL.append(", [").append(sanitizedField).append("] TEXT(255)");
            }
            createTableSQL.append(")");
            
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createTableSQL.toString());
                
                List<String> fieldNames = new ArrayList<>(fieldMapping.values());
                int recordCount = 0;
                
                for (Map<String, String> record : records) {
                    StringBuilder insertSQL = new StringBuilder();
                    insertSQL.append("INSERT INTO dataBase (");
                    insertSQL.append(String.join(", ", fieldNames.stream()
                        .map(f -> "[" + sanitizeFieldName(f) + "]")
                        .collect(Collectors.toList())));
                    insertSQL.append(") VALUES (");
                    insertSQL.append(String.join(", ", Collections.nCopies(fieldNames.size(), "?")));
                    insertSQL.append(")");
                    
                    try (PreparedStatement pstmt = conn.prepareStatement(insertSQL.toString())) {
                        int paramIndex = 1;
                        for (String field : fieldNames) {
                            pstmt.setString(paramIndex++, record.getOrDefault(field, ""));
                        }
                        pstmt.executeUpdate();
                        recordCount++;
                    }
                }
                System.out.println("Inserted " + recordCount + " records into database");
            }
        } catch (SQLException e) {
            System.err.println("Error writing to MDB: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void writeToCSV(List<Map<String, String>> records, String outputFile) {
        if (records.isEmpty()) return;
        
        try (FileWriter writer = new FileWriter(outputFile)) {
            Set<String> headers = new HashSet<>();
            records.forEach(record -> headers.addAll(record.keySet()));
            writer.write(String.join(",", headers) + "\n");

            for (Map<String, String> record : records) {
                List<String> values = new ArrayList<>();
                for (String header : headers) {
                    String value = record.getOrDefault(header, "");
                    value = value.replace("\"", "\"\"");
                    if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
                        value = "\"" + value + "\"";
                    }
                    values.add(value);
                }
                writer.write(String.join(",", values) + "\n");
            }
            System.out.println("CSV file created at: " + outputFile);
        } catch (IOException e) {
            System.err.println("Error writing CSV: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
