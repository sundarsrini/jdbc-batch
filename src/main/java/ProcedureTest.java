

import java.sql.*;

public class ProcedureTest {

    public static void main(String args[]) throws SQLException {
        new ProcedureTest().prepareProcedureAndCall();
    }

    public void prepareProcedureAndCall() throws SQLException {

        String sql = "DELIMITER $$\n" +
                "\n" +
                "DROP PROCEDURE IF EXISTS test $$\n" +
                "CREATE PROCEDURE test (id INT, name VARCHAR(40))\n" +
                "BEGIN\n" +
                "\n" +
                "  DECLARE code CHAR(5) DEFAULT '00000';\n" +
                "  DECLARE msg VARCHAR(64);\n" +
                "  DECLARE nrows INT;\n" +
                "  DECLARE result TEXT;\n" +
                "\n" +
                "DECLARE CONTINUE HANDLER FOR SQLEXCEPTION\n" +
                "    BEGIN\n" +
                "      GET DIAGNOSTICS CONDITION 1\n" +
                "        code = RETURNED_SQLSTATE, msg = MESSAGE_TEXT;\n" +
                "    END;\n" +
                "INSERT INTO error_handling (id, name) VALUES (id, name); \n" +
                "IF code = '00000' THEN\n" +
                "    GET DIAGNOSTICS nrows = ROW_COUNT;\n" +
                "    SET result = CONCAT('insert succeeded, row count = ',nrows);\n" +
                "  ELSE\n" +
                "    SET result = CONCAT('insert failed, error = ',code,', message = ',msg);\n" +
                "    INSERT INTO error_table (result) VALUES (result);\n" +
                "  END IF;\n" +
                "  SELECT result;\n" +
                "\n" +
                "END $$\n" +
                "\n" +
                "DELIMITER ;";

        System.out.println(sql);

        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/batch", "root", "password");

        CallableStatement callableStatement = connection.prepareCall("call test(?,?)");


        for(int i=0;i<10000;i++){
            if(i%2 == 0){
                callableStatement.setInt(1, i);
                callableStatement.setString(2, "Sundar");
            } else {
                callableStatement.setInt(1, i);
                callableStatement.setString(2, null);
            }
            callableStatement.addBatch();

        }
//
//        Statement statement = connection.createStatement();
//        ResultSet rs = statement.executeQuery("select * from error_handling");
//        ResultSetMetaData rsmd = rs.getMetaData();
//
//        System.out.println("No. of columns : " + rsmd.getColumnCount());
//        System.out.println("Column name of 1st column : " + rsmd.getColumnName(1));
//        System.out.println("Column type of 1st column : " + rsmd.getColumnTypeName(1));

        try{
            long startTime = System.currentTimeMillis();
            callableStatement.executeBatch();
            long endTime = System.currentTimeMillis();
            System.out.println("Total Execution Time: "+ (endTime - startTime));

        } catch (SQLException e){
            System.out.println(e.getMessage());

        }

        connection.close();
//        callableStatement.close();
    }
}
