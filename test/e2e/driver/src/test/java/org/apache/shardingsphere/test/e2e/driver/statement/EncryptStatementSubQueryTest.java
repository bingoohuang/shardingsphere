package org.apache.shardingsphere.test.e2e.driver.statement;


import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.Objects;
import lombok.Cleanup;
import org.apache.shardingsphere.driver.api.yaml.YamlShardingSphereDataSourceFactory;
import org.apache.shardingsphere.driver.jdbc.core.datasource.ShardingSphereDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class EncryptStatementSubQueryTest   {

    @Test
    void student() throws SQLException {
        @Cleanup Connection c = getEncryptConnection();
        executeDDL(c, "CREATE TABLE IF NOT EXISTS student (id varchar(32), name varchar(255), addr varchar(255), email varchar(255), phone varchar(255), age int(3), id_card varchar(256), PRIMARY KEY (id))");
        executeDDL(c, "CREATE TABLE IF NOT EXISTS teacher (id varchar(32), name varchar(255), addr varchar(255), email varchar(255), phone varchar(255), age int(3), id_card varchar(256), PRIMARY KEY (id))");
        executeDDL(c, "truncate table student");
        executeDDL(c,"truncate table teacher");
        executeUpdate(c, "insert into student (id, gender, teacher_id, name, addr, email, phone, age, id_card, birthday) "
                                     + "values ('1', '男','1', '居菁茗', '宁国二支路123号-7-5', 'g0wtq739kdacsg@0355.net', '15006186538', 31, '500103198211274175', '2023年08月25日')");
        executeUpdate(c, "insert into student (id, gender, teacher_id, name, addr, email, phone, age, id_card, birthday) "
                                     + "values ('2', '女','1', '学生2号', '京山广场50号-4-5', '99q3dwc1@live.com', '15106553952', 15, '32120319860719617X', '2020年08月25日')");
        executeUpdate(c, "insert into teacher (id, gender, name, addr, email, phone, age, id_card, birthday) "
                                     + "values ('1', '女', '居菁茗', '台北路111号-18-1', 'omb541s9j8hw1@live.com', '13006445062', 98, '321200197702152315', '2010年08月25日')");

        String s1 = "update student set phone = ?  where name = (select name from teacher where name = ? and name = '居菁茗' ) and name = ?";
        executeUpdate(c, s1, "18610002000", "居菁茗", "居菁茗");
        String s2 = "update student set phone = (select phone from teacher where id = student.teacher_id and name = ?) where name = ?";
        executeUpdate(c, s2, "居菁茗", "居菁茗");
        String s4 = "select id, teacher_id, (select name from student where name =?) as name, gender, birthday, phone, addr from student where name =?";
        executeQuery(c, s4, "居菁茗", "居菁茗");
        String s3 = "delete from student where name = (select name from teacher where name = ?)";
        executeUpdate(c, s3, "居菁茗");
    }

    private static void executeQuery(Connection c, String s1, Object... args) throws SQLException {
        try (PreparedStatement p = c.prepareStatement(s1)) {
            for (int i = 0; i < args.length; i++) {
                p.setObject(i + 1, args[i]);
            }
            ResultSet rs = p.executeQuery();

            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            int rowIndex = 0;
            while (rs.next()) {
                rowIndex++;
                System.out.print("row " + rowIndex + ": ");
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = rs.getString(i);
                    System.out.print(rsmd.getColumnName(i) + ": " + columnValue);
                }
                System.out.println();
            }
        }
    }

    private static void executeDDL(Connection c, String s1) throws SQLException {
        try (Statement p = c.createStatement()) {
            p.execute(s1);
        }
    }
    private static void executeUpdate(Connection c, String s1, Object... args) throws SQLException {
        try (PreparedStatement p = c.prepareStatement(s1)) {
            for (int i = 0; i < args.length; i++) {
                p.setObject(i + 1, args[i]);
            }
            int updated = p.executeUpdate();
            System.out.println("executeUpdate: " + updated);
        }
    }

    private static ShardingSphereDataSource queryWithCipherDataSource;


    private static final String CONFIG_FILE_WITH_QUERY_WITH_CIPHER = "config/config-encrypt-subquery.yaml";

    @BeforeAll
    static void initEncryptDataSource() throws SQLException, IOException {
        if (null != queryWithCipherDataSource) {
            return;
        }
        File file = new File(Objects.requireNonNull(
                Thread.currentThread().getContextClassLoader().getResource(CONFIG_FILE_WITH_QUERY_WITH_CIPHER), String.format("File `%s` is not existed.", CONFIG_FILE_WITH_QUERY_WITH_CIPHER)).getFile());
        queryWithCipherDataSource = (ShardingSphereDataSource) YamlShardingSphereDataSourceFactory.createDataSource( file);
    }


    protected final Connection getEncryptConnection() {
        return queryWithCipherDataSource.getConnection();
    }


    @AfterAll
    static void close() throws Exception {
        if (null == queryWithCipherDataSource) {
            return;
        }
        queryWithCipherDataSource.close();
        queryWithCipherDataSource = null;
    }

}
