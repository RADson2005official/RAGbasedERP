import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

/**
 * Database utility class for importing any Excel file into database
 */
public class ExcelDB {
    // Database connection parameters
    private static final String DB_URL = "jdbc:mysql://localhost:3306/project1";
    private static final String USER = "root";
    private static final String PASSWORD = "System@305";
    
    /**
     * Main method to import Excel data into database
     * @param args command line arguments (optional)
     */
    public static void main(String[] args) {
        // Create a Scanner for user input
        Scanner scanner = new Scanner(System.in);
        
        // Default values
        String excelFilePath;
        String tableName;
        
        // Prompt for Excel file path
        System.out.print("Enter path to Excel file: ");
        excelFilePath = scanner.nextLine().trim();
        
        // Prompt for table name
        System.out.print("Enter database table name: ");
        tableName = scanner.nextLine().trim();
        
        // Use defaults if no input provided
        if (excelFilePath.isEmpty()) {
            excelFilePath = "data.xlsx";
            System.out.println("Using default file: " + excelFilePath);
        }
        
        if (tableName.isEmpty()) {
            tableName = "excel_data";
            System.out.println("Using default table: " + tableName);
        }
        
        try {
            // Register the JDBC driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            
            // Using try-with-resources to ensure connection is closed automatically
            try (Connection connection = DriverManager.getConnection(DB_URL, USER, PASSWORD)) {
                System.out.println("Connecting to database...");
                System.out.println("Connection successful!");
                
                // Import data from Excel to Database
                importExcelToDatabase(connection, excelFilePath, tableName);
                
                System.out.println("Excel data successfully imported to database table: " + tableName);
            }
            
        } catch (ClassNotFoundException e) {
            System.err.println("MySQL JDBC Driver not found!");
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Database error!");
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Error reading Excel file!");
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("An unexpected error occurred!");
            e.printStackTrace();
        } finally {
            scanner.close();
        }
    }
    
    /**
     * Imports data from Excel file to database by dynamically creating a table if needed
     * @param connection database connection
     * @param excelFilePath path to the Excel file
     * @param tableName name of the table to insert data into
     * @throws SQLException if a database access error occurs
     * @throws IOException if an I/O error occurs
     */
    private static void importExcelToDatabase(Connection connection, String excelFilePath, String tableName) 
            throws SQLException, IOException {
        
        System.out.println("\nImporting data from Excel file: " + excelFilePath);
        
        File excelFile = new File(excelFilePath);
        if (!excelFile.exists()) {
            throw new IOException("Excel file not found: " + excelFilePath);
        }
        
        // Create a FileInputStream object for the Excel file
        try (FileInputStream fis = new FileInputStream(excelFile)) {
            
            // Determine the type of Excel file (.xls or .xlsx)
            Workbook workbook;
            if (excelFilePath.toLowerCase().endsWith("xlsx")) {
                workbook = new XSSFWorkbook(fis);
            } else if (excelFilePath.toLowerCase().endsWith("xls")) {
                workbook = new HSSFWorkbook(fis);
            } else {
                throw new IOException("Not a valid Excel file format. File must be .xls or .xlsx");
            }
            
            try {
                // Get the first sheet from the workbook
                Sheet sheet = workbook.getSheetAt(0);
                
                // Get the first row to determine column headers
                Row headerRow = sheet.getRow(0);
                if (headerRow == null) {
                    throw new IOException("Empty Excel file or no header row found");
                }
                
                // Extract column names from header row
                List<String> columnNames = new ArrayList<>();
                for (int i = 0; i < headerRow.getLastCellNum(); i++) {
                    Cell cell = headerRow.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                    String columnName = getCellValueAsString(cell).trim();
                    
                    // If column name is empty, generate a default name
                    if (columnName.isEmpty()) {
                        columnName = "column_" + (i + 1);
                    }
                    
                    // Clean the column name (remove spaces and special characters)
                    columnName = columnName.replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase();
                    
                    columnNames.add(columnName);
                }
                
                // Create table if it doesn't exist
                createTableIfNotExists(connection, tableName, columnNames);
                
                // Prepare SQL for inserting data
                StringBuilder insertSql = new StringBuilder("INSERT INTO " + tableName + " (");
                StringBuilder placeholders = new StringBuilder(") VALUES (");
                
                for (int i = 0; i < columnNames.size(); i++) {
                    insertSql.append(columnNames.get(i));
                    placeholders.append("?");
                    
                    if (i < columnNames.size() - 1) {
                        insertSql.append(", ");
                        placeholders.append(", ");
                    }
                }
                insertSql.append(placeholders).append(")");
                
                // Insert data from Excel to database
                try (PreparedStatement preparedStatement = connection.prepareStatement(insertSql.toString())) {
                    int rowCount = 0;
                    
                    // Start from the second row (index 1) as the first row is header
                    for (int rowNum = 1; rowNum <= sheet.getLastRowNum(); rowNum++) {
                        Row row = sheet.getRow(rowNum);
                        if (row == null) continue;
                        
                        // Check if row is empty
                        boolean isRowEmpty = true;
                        for (int colNum = 0; colNum < columnNames.size(); colNum++) {
                            Cell cell = row.getCell(colNum, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                            if (cell != null && cell.getCellType() != CellType.BLANK) {
                                isRowEmpty = false;
                                break;
                            }
                        }
                        
                        if (isRowEmpty) continue;
                        
                        // Process each cell in the row
                        for (int colNum = 0; colNum < columnNames.size(); colNum++) {
                            Cell cell = row.getCell(colNum, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                            setCellValueToPreparedStatement(preparedStatement, cell, colNum + 1);
                        }
                        
                        // Execute insert
                        preparedStatement.executeUpdate();
                        rowCount++;
                        
                        // Print progress for large files
                        if (rowCount % 100 == 0) {
                            System.out.println("Processed " + rowCount + " rows...");
                        }
                    }
                    
                    System.out.println("Imported " + rowCount + " rows into table " + tableName);
                }
            } finally {
                workbook.close();
            }
        }
    }
    
    /**
     * Creates a database table if it doesn't exist based on Excel column headers
     * @param connection database connection
     * @param tableName name of the table to create
     * @param columnNames list of column names from Excel
     * @throws SQLException if a database access error occurs
     */
    private static void createTableIfNotExists(Connection connection, String tableName, List<String> columnNames) 
            throws SQLException {
        
        StringBuilder createTableSql = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (");
        createTableSql.append("id INT AUTO_INCREMENT PRIMARY KEY, ");
        
        for (int i = 0; i < columnNames.size(); i++) {
            createTableSql.append(columnNames.get(i)).append(" TEXT");
            
            if (i < columnNames.size() - 1) {
                createTableSql.append(", ");
            }
        }
        
        createTableSql.append(")");
        
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql.toString());
            System.out.println("Table structure verified/created: " + tableName);
            System.out.println("Table has " + (columnNames.size() + 1) + " columns (including id)");
        }
    }
    
    /**
     * Sets cell value to prepared statement based on cell type
     * @param preparedStatement prepared statement for SQL execution
     * @param cell Excel cell
     * @param paramIndex parameter index in prepared statement
     * @throws SQLException if a database access error occurs
     */
    private static void setCellValueToPreparedStatement(PreparedStatement preparedStatement, Cell cell, int paramIndex) 
            throws SQLException {
        
        if (cell == null || cell.getCellType() == CellType.BLANK) {
            preparedStatement.setNull(paramIndex, java.sql.Types.VARCHAR);
            return;
        }
        
        switch (cell.getCellType()) {
            case STRING:
                preparedStatement.setString(paramIndex, cell.getStringCellValue());
                break;
                
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    preparedStatement.setDate(paramIndex, new java.sql.Date(cell.getDateCellValue().getTime()));
                } else {
                    preparedStatement.setDouble(paramIndex, cell.getNumericCellValue());
                }
                break;
                
            case BOOLEAN:
                preparedStatement.setBoolean(paramIndex, cell.getBooleanCellValue());
                break;
                
            case FORMULA:
                try {
                    switch (cell.getCachedFormulaResultType()) {
                        case STRING:
                            preparedStatement.setString(paramIndex, cell.getStringCellValue());
                            break;
                        case NUMERIC:
                            if (DateUtil.isCellDateFormatted(cell)) {
                                preparedStatement.setDate(paramIndex, new java.sql.Date(cell.getDateCellValue().getTime()));
                            } else {
                                preparedStatement.setDouble(paramIndex, cell.getNumericCellValue());
                            }
                            break;
                        case BOOLEAN:
                            preparedStatement.setBoolean(paramIndex, cell.getBooleanCellValue());
                            break;
                        default:
                            preparedStatement.setNull(paramIndex, java.sql.Types.VARCHAR);
                    }
                } catch (Exception e) {
                    // Handle formula evaluation errors by using the formula string
                    preparedStatement.setString(paramIndex, cell.getCellFormula());
                }
                break;
                
            case ERROR:
                preparedStatement.setString(paramIndex, "ERROR");
                break;
                
            default:
                preparedStatement.setNull(paramIndex, java.sql.Types.VARCHAR);
        }
    }
    
    /**
     * Utility method to get cell value as string regardless of cell type
     * @param cell Excel cell
     * @return String representation of cell value
     */
    private static String getCellValueAsString(Cell cell) {
        if (cell == null) {
            return "";
        }
        
        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getDateCellValue().toString();
                } else {
                    // Check if it's an integer or double
                    double value = cell.getNumericCellValue();
                    if (value == Math.floor(value)) {
                        return String.valueOf((long)value);
                    }
                    return String.valueOf(value);
                }
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                try {
                    return cell.getStringCellValue();
                } catch (Exception e) {
                    try {
                        return String.valueOf(cell.getNumericCellValue());
                    } catch (Exception ex) {
                        return cell.getCellFormula();
                    }
                }
            default:
                return "";
        }
    }
}=9o;