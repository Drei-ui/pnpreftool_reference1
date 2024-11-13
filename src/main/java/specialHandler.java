import java.io.*;
import java.sql.*;
import java.util.*;
import java.text.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;
import java.nio.file.*;

public class specialHandler {

    public static void main(String[] args) {
        Properties config = loadConfig("config/config.properties");

        String inputPath = config.getProperty("Input_path");
        String templateMdbPath = config.getProperty("Template_MDB");
        String schemaMdbPath = config.getProperty("Schema_MDB");
        String outputMdbPath = config.getProperty("Output_MDB");
        String inputExt = config.getProperty("input_ext", "QCA");
        String outputPath = config.getProperty("Output_path");

        // Print paths for verification
        System.out.println("Input Path: " + inputPath);
        System.out.println("Template MDB Path: " + templateMdbPath);
        System.out.println("Schema MDB Path: " + schemaMdbPath);
        System.out.println("Output MDB Path: " + outputMdbPath);
        System.out.println("Output Path: " + outputPath);

        List<String> docTypes = Arrays.asList("SI", "CSR", "SBR");

        Map<String, String> fieldNumberToName = getFieldNumberToNameMapping(schemaMdbPath, docTypes);

        File inputDir = new File(inputPath);
        File[] qcaFiles = inputDir.listFiles((dir, name) -> name.toLowerCase().endsWith("." + inputExt.toLowerCase()));

        //create an output directory if given directory is non existent
        try{
            Path outputDir = Paths.get(outputPath);
            Files.createDirectories(outputDir);
            System.out.println("Output path at: " + outputDir);
        } catch(Exception e){
            System.err.println("Error creating output directory: " + e.getMessage());
            e.printStackTrace();
        }
        // Copy template MDB to output location
        try {
            // Get the filename from the Output_MDB path
            String outputFileName = Paths.get(outputMdbPath).getFileName().toString();
            
            // Combine Output_path with the filename
            Path fullOutputPath = Paths.get(outputPath, outputFileName);
            
            // Copy template MDB to output location
            Files.copy(Paths.get(templateMdbPath), fullOutputPath, 
                      StandardCopyOption.REPLACE_EXISTING);
            System.out.println("Template database copied from: " + templateMdbPath);
            System.out.println("Template database copied to: " + fullOutputPath);
            
            // Update outputMdbPath for later use
            outputMdbPath = fullOutputPath.toString();
        } catch (IOException e) {
            System.err.println("Error copying template database: " + e.getMessage());
            System.err.println("Template path: " + templateMdbPath);
            System.err.println("Output path: " + outputMdbPath);
            e.printStackTrace();
            return;
        }

        if (qcaFiles != null && qcaFiles.length > 0) {
            System.out.println("Found " + qcaFiles.length + " " + inputExt + " files");
            for (File qcaFile : qcaFiles) {
                System.out.println("Processing file: " + qcaFile.getName());

                Map<String, String> fieldValues = extractFieldValuesFromXml(qcaFile, fieldNumberToName);

                System.out.println("Extracted Field Values: " + fieldValues);

                insertValuesIntoOutputMdb(outputMdbPath, fieldValues);
            }
        } else {
            System.out.println("No QCA files found in the directory.");
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

    private static Map<String, String> getFieldNumberToNameMapping(String schemaMdbPath, List<String> docTypes) {
        Map<String, String> fieldNumberToName = new HashMap<>();
        String url = "jdbc:ucanaccess://" + schemaMdbPath;
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            
            // Get all fields from the schema table with a more inclusive query
            String query = "SELECT FieldNo, FieldName, DocType FROM tblFields";
            ResultSet rs = stmt.executeQuery(query);
            
            while (rs.next()) {
                String fieldNo = rs.getString("FieldNo");
                String fieldName = rs.getString("FieldName");
                String docType = rs.getString("DocType");
                
                // Clean up the field number
                fieldNo = fieldNo.replace(".0", "").trim();
                
                // Add mapping without document type for all fields
                fieldNumberToName.put(fieldNo, fieldName);
                
                // Also add mapping with document type if present
                if (docType != null) {
                    docType = docType.replace("~", "").trim();
                    String mappingKey = fieldNo + "-" + docType;
                    fieldNumberToName.put(mappingKey, fieldName);
                }
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

    private static void insertValuesIntoOutputMdb(String outputMdbPath, Map<String, String> fieldValues) {
        // Don't process if we have no values
        if (fieldValues.isEmpty()) {
            System.out.println("No field values to insert - skipping record");
            return;
        }

        String url = "jdbc:ucanaccess://" + outputMdbPath;
        
        try (Connection conn = DriverManager.getConnection(url)) {
            String sql = "INSERT INTO Liaison ([App Date], [Type], [Make], [Model], [Year], [Color], " +
                        "[Plate #], [Engine #], [Chasis #], [MVCO OR #], [Owner], [TIN Owner], [Address], [Purpose], " +
                        "[Acquired From], [From TIN], [From Address], [Switch], [MacroEch], [Operator Id]) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                // Parse and format the date with error handling
                try {
                    String dateSold = fieldValues.get("Date Sold");
                    if (dateSold != null && !dateSold.trim().isEmpty()) {
                        SimpleDateFormat inputFormat = new SimpleDateFormat("MM/dd/yyyy");
                        inputFormat.setLenient(false);
                        java.util.Date date = inputFormat.parse(dateSold);
                        pstmt.setDate(1, new java.sql.Date(date.getTime()));
                    } else {
                        pstmt.setNull(1, Types.DATE);
                    }
                } catch (ParseException e) {
                    System.err.println("Error parsing date: " + e.getMessage());
                    pstmt.setNull(1, Types.DATE);
                }

                // Type (field #2)
                String bodyType = fieldValues.getOrDefault("BODY TYPE", "UNKNOWN");
                pstmt.setString(2, bodyType.length() > 20 ? bodyType.substring(0, 17) + "..." : bodyType);

                // Make (field #3)
                pstmt.setString(3, fieldValues.getOrDefault("Make", "TOYOTA"));

                // Model (field #4)
                String model = fieldValues.getOrDefault("Model", "UNKNOWN");
                pstmt.setString(4, model.length() > 20 ? model.substring(0, 17) + "..." : model);

                // Year (field #5)
                pstmt.setString(5, fieldValues.getOrDefault("YEAR", "2024"));

                // Color (field #6)
                String color = fieldValues.getOrDefault("Color", "NOT SPECIFIED");
                pstmt.setString(6, color.length() > 20 ? color.substring(0, 17) + "..." : color);

                // Plate # (field #7) - ensure not empty
                String plateNo = fieldValues.getOrDefault("Conduction Sitcker No./Plate Number", "PENDING");
                if (plateNo.trim().isEmpty()) {
                    plateNo = "PENDING";
                }
                pstmt.setString(7, plateNo);

                // Engine # (field #8)
                pstmt.setString(8, fieldValues.getOrDefault("ENGINE NO", ""));

                // Chassis # (field #9)
                pstmt.setString(9, fieldValues.getOrDefault("CHASSIS#", ""));

                // MVCO OR # (field #10) - hardcoded as per requirement
                pstmt.setString(10, "21201218");

                // Owner (field #11)
                StringBuilder ownerName = new StringBuilder();
                String lastName = fieldValues.getOrDefault("Last Name", "");
                String firstName = fieldValues.getOrDefault("First Name", "");
                String middleName = fieldValues.getOrDefault("Middle Name", "");
                String extension = fieldValues.getOrDefault("Extension", "");
                
                if (!lastName.isEmpty()) ownerName.append(lastName);
                if (!firstName.isEmpty()) ownerName.append(", ").append(firstName);
                if (!middleName.isEmpty()) ownerName.append(" ").append(middleName);
                if (!extension.isEmpty()) ownerName.append(" ").append(extension);
                
                String owner = ownerName.toString();
                if (owner.isEmpty()) owner = "NOT SPECIFIED";
                pstmt.setString(11, owner.length() > 50 ? owner.substring(0, 47) + "..." : owner);

                // TIN Owner (field #12)
                String tin = fieldValues.getOrDefault("TIN", "000-000-000-000");
                pstmt.setString(12, tin);

                // Address (field #13)
                List<String> addressParts = new ArrayList<>();
                String[] addressFields = {
                    "Unit No./HouseNo./Floor No.",
                    "Building",
                    "Street",
                    "Barangay",
                    "City or Municipality",
                    "Province",
                    "Zipcode"
                };
                for (String field : addressFields) {
                    String part = fieldValues.get(field);
                    if (part != null && !part.trim().isEmpty()) {
                        addressParts.add(part.trim());
                    }
                }
                String address = addressParts.isEmpty() ? "NOT SPECIFIED" : String.join(" ", addressParts);
                pstmt.setString(13, address.length() > 100 ? address.substring(0, 97) + "..." : address);

                // Fixed values for remaining fields
                pstmt.setString(14, "New Registration");               // Purpose
                pstmt.setString(15, "TOYOTA OTIS INC");               // Acquired From
                pstmt.setString(16, "003-498-557-000");               // From TIN
                pstmt.setString(17, "1770 P M GUAZON ST");            // From Address
                pstmt.setString(18, "0");                             // Switch
                pstmt.setString(19, fieldValues.getOrDefault("CSR NUMBER", "")); // MacroEch
                pstmt.setString(20, "MVCO");                          // Operator Id

                pstmt.executeUpdate();
                System.out.println("Record inserted successfully");
                
            } catch (SQLException e) {
                System.err.println("Error inserting record: " + e.getMessage());
                e.printStackTrace();
            }
        } catch (SQLException e) {
            System.err.println("Database connection error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
