package com.dcs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class DCSDb {
    Logger LOGGER = LoggerFactory.getLogger(DCSDb.class);
    String db_url = "//tmp/dcs.db";
    Connection conn;
    List<String> object_cols;
    String new_rec_stmt;
    PreparedStatement pstmt;
    int batch_stmt_sz = 0;
    int max_batch = 1000;
    int total_writes = 0;

    public DCSDb() {
        File file = new File(this.db_url);
        if (file.exists()) {
            if (file.delete()) {
                LOGGER.info("Existing database deleted successfully!");
            } else {
                LOGGER.error("Failed to delete the existing database!");
            }
        }
        this.conn = this.connect();
        LOGGER.info("Creating database at {}...", this.db_url);
        createObjectTable();
        sqliteTableColumns();
        createNewInsertStmt();
        LOGGER.info("Database created successfully...");
    }

    private Connection connect() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:sqlite:" + this.db_url);
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                LOGGER.info("{}", meta);
            }
        } catch (SQLException e) {
            LOGGER.error("Could not create sql connection!\n{}", e.getMessage());
        }
        return conn;
    }

    private void createUpdateStatement(String id, String field, Object value) {
        String sql = String.format("UPDATE object SET %s = ? WHERE id = ?", field);
        try {
            PreparedStatement upd_stmt = this.conn.prepareStatement(sql);
            upd_stmt.setObject(1, value);
            upd_stmt.setObject(2, id);
            upd_stmt.execute();
        } catch (SQLException e) {
            LOGGER.info("Could not update {}-{} of id: {}\n{}", field, value, id, e.getMessage());
        }
    }

    private void createNewInsertStmt() {
        StringBuilder quests = new StringBuilder();
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO object(");
        for (int i = 0; i < this.object_cols.size(); i++) {
            if (i > 0) {
                sqlBuilder.append(",");
                quests.append(",");
            }
            sqlBuilder.append(this.object_cols.get(i));
            quests.append("?");
        }
        this.new_rec_stmt = sqlBuilder.toString() + ") VALUES(" + quests + ")";
    }

    public void insertNewObject(DCSObject object) {
        LOGGER.debug("Exporting dcs object to database....");
        try {
            if (this.pstmt == null) {
                this.pstmt = this.conn.prepareStatement(this.new_rec_stmt);
            }
            for (int i = 0; i < this.object_cols.size(); i++) {
                try {
                    pstmt.setObject(i + 1, object.getValue(this.object_cols.get(i)));
                } catch (ArrayIndexOutOfBoundsException e) {
                    pstmt.setObject(i + 1, null);
                }
                this.total_writes++;
            }
            this.pstmt.addBatch();
            this.batch_stmt_sz += 1;
            this.flushBatchToDatabase(false);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }
    }

    public void flushBatchToDatabase(boolean force) {
        if (force || (this.batch_stmt_sz % this.max_batch == 0)) {
            LOGGER.info("Flushing batch to sqlite...");
            try {
                this.pstmt.executeBatch();
                this.batch_stmt_sz = 0;
                this.pstmt = null;
//                this.conn.close();
            } catch (SQLException e) {
                LOGGER.error("Error flushing entries!\n{}", e.getMessage());
            }
        }
    }

    public void createObjectTable() {
        LOGGER.info("Creating dcs.db objects table...");
        String sql = "CREATE TABLE object (" +
                "id VARCHAR, " +
                "session_id VARCHAR, " +
                "name VARCHAR, " +
                "color VARCHAR, " +
                "grp VARCHAR, " +
                "pilot VARCHAR, " +
                "platform VARCHAR, " +
                "type VARCHAR, " +
                "alive VARCHAR, " +
                "first_seen TIMESTAMP, " +
                "last_seen TIMESTAMP, " +
                "coalition VARCHAR, " +
                "lat FLOAT, " +
                "lon FLOAT, " +
                "alt FLOAT DEFAULT 1.0, " +
                "roll FLOAT, " +
                "pitch FLOAT, " +
                "yaw FLOAT, " +
                "u_coord FLOAT, " +
                "v_coord FLOAT, " +
                "heading FLOAT, " +
                "updates INTEGER, " +
                "parent varchar, " +
                "parent_dist FLOAT " +
                ");";

        LOGGER.info(sql);
        try (Statement stmt = this.conn.createStatement()) {
            stmt.execute(sql);
            String schema = this.conn.getSchema();
            LOGGER.info(schema);
        } catch (SQLException e) {
            LOGGER.error("Objects table not created!");
            LOGGER.error(e.getMessage());
        }
    }

    private void sqliteTableColumns() {
        String tableName = "object";
        List<String> columns = new ArrayList<>();
        String sql = "select * from " + tableName + " LIMIT 0";
        try (Statement statement = this.conn.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);
            ResultSetMetaData mrs = rs.getMetaData();
            for (int i = 1; i <= mrs.getColumnCount(); i++) {
                columns.add(mrs.getColumnLabel(i));
            }
        } catch (SQLException e) {
            LOGGER.error("Schema columns not collected!\n{}", e.getMessage());
        }
        LOGGER.info("Columns found: {}", columns);
        this.object_cols = columns;
    }
}