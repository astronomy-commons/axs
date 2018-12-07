package org.dirac.axs.util;

import java.sql.*;
import java.util.*;

import org.apache.spark.sql.SparkSession;

public class CatalogUtils {
    private static String DRIVER_CLSS = null;
    private static String DB_URL = null;
    private static String DB_USER = null;
    private static String DB_PASS = null;

    public static void setup(SparkSession spark) {
        String driver = spark.sparkContext().hadoopConfiguration().get("javax.jdo.option.ConnectionDriverName");
        if(driver == null || "".equals(driver)) {
            setCredentials(
                    "org.apache.derby.jdbc.EmbeddedDriver",
                    "jdbc:derby:metastore_db;create=true",
                    "",
                    "");
        } else {
            setCredentials(
                spark.sparkContext().hadoopConfiguration().get("javax.jdo.option.ConnectionDriverName"),
                spark.sparkContext().hadoopConfiguration().get("javax.jdo.option.ConnectionURL"),
                spark.sparkContext().hadoopConfiguration().get("javax.jdo.option.ConnectionUserName"),
                spark.sparkContext().hadoopConfiguration().get("javax.jdo.option.ConnectionPassword"));
        }
    }
    public static void setCredentials(String driverCls, String url, String user, String pass) {
        DRIVER_CLSS = driverCls;
        DB_URL = url;
        DB_USER = user;
        DB_PASS = pass;
    }

    private static Connection getConnection() throws Exception {
        try {
            Class.forName(DRIVER_CLSS);
            Connection conn = java.sql.DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
            return conn;
        } catch(Exception e) {
            e.printStackTrace();
            throw new Exception("Configuration error: connection not available.");
        }
    }
    
    public static boolean updateSparkMetastoreBucketing(String tableName, int numBuckets) throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();

        try {
            int tblid = getSparkTableId(tableName.toLowerCase(), stmt);
            if(tblid == -1)
                throw new Exception("Table "+tableName+" does not exist.");
            try {
                try {
                    stmt.executeUpdate("insert into TABLE_PARAMS (TBL_ID, PARAM_KEY, PARAM_VALUE) VALUES " +
                        "("+tblid+", 'spark.sql.sources.schema.numSortCols', '2')");
                } catch(SQLException sqle) {
                    stmt.executeUpdate("update TABLE_PARAMS SET PARAM_VALUE = '2'" +
                            "where TBL_ID = "+tblid+" AND PARAM_KEY = 'spark.sql.sources.schema.numSortCols'");
                }
                try {
                    stmt.executeUpdate("insert into TABLE_PARAMS (TBL_ID, PARAM_KEY, PARAM_VALUE) VALUES " +
                            "("+tblid+", 'spark.sql.sources.schema.numBuckets', '"+numBuckets+"')");
                } catch(SQLException sqle) {
                    stmt.executeUpdate("update TABLE_PARAMS SET PARAM_VALUE = '"+numBuckets+"'" +
                            "where TBL_ID = "+tblid+" AND PARAM_KEY = 'spark.sql.sources.schema.numBuckets'");
                }
                try {
                    stmt.executeUpdate("insert into TABLE_PARAMS (TBL_ID, PARAM_KEY, PARAM_VALUE) VALUES " +
                            "("+tblid+", 'spark.sql.sources.schema.numBucketCols', '1')");
                } catch(SQLException sqle) {
                    stmt.executeUpdate("update TABLE_PARAMS SET PARAM_VALUE = '1'" +
                            "where TBL_ID = "+tblid+" AND PARAM_KEY = 'spark.sql.sources.schema.numBucketCols'");
                }
                try {
                    stmt.executeUpdate("insert into TABLE_PARAMS (TBL_ID, PARAM_KEY, PARAM_VALUE) VALUES " +
                            "("+tblid+", 'spark.sql.sources.schema.bucketCol.0', 'zone')");
                } catch(SQLException sqle) {
                    stmt.executeUpdate("update TABLE_PARAMS SET PARAM_VALUE = 'zone'" +
                            "where TBL_ID = "+tblid+" AND PARAM_KEY = 'spark.sql.sources.schema.bucketCol.0'");
                }
                try {
                    stmt.executeUpdate("insert into TABLE_PARAMS (TBL_ID, PARAM_KEY, PARAM_VALUE) VALUES " +
                            "("+tblid+", 'spark.sql.sources.schema.sortCol.0', 'zone')");
                } catch(SQLException sqle) {
                    stmt.executeUpdate("update TABLE_PARAMS SET PARAM_VALUE = 'zone'" +
                            "where TBL_ID = "+tblid+" AND PARAM_KEY = 'spark.sql.sources.schema.sortCol.0'");
                }
                try {
                    stmt.executeUpdate("insert into TABLE_PARAMS (TBL_ID, PARAM_KEY, PARAM_VALUE) VALUES " +
                            "("+tblid+", 'spark.sql.sources.schema.sortCol.1', 'ra')");
                } catch(SQLException sqle) {
                    stmt.executeUpdate("update TABLE_PARAMS SET PARAM_VALUE = 'ra'" +
                            "where TBL_ID = "+tblid+" AND PARAM_KEY = 'spark.sql.sources.schema.sortCol.1'");
                }
                return true;
            } catch(Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        finally {
            try { stmt.close(); } catch(Exception ee) { }
            try { conn.close(); } catch(Exception ee) { }
        }
    }

    private static void createDbTableIfNotExists(Statement stmt) throws Exception {
        try {
            stmt.executeQuery("select count(*) from AXSTABLES");
        } catch(SQLException sqle) {
            stmt.executeUpdate("create table AXSTABLES (" +
                    "TBL_ID integer NOT NULL PRIMARY KEY, " +
                    "TBL_NAME varchar(128) NOT NULL UNIQUE, " +
                    "NUM_BUCKETS integer NOT NULL, " +
                    "ZONE_HEIGHT decimal(13,10) NOT NULL, " +
                    "BUCKET_COL varchar(128) NOT NULL, " +
                    "RA_COL varchar(128) NOT NULL, " +
                    "DEC_COL varchar(128) NOT NULL, " +
                    "LC smallint NOT NULL, " +
                    "LC_COLS VARCHAR(10000))");
        }
    }

    private static int getSparkTableId(String tableName, Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT TBL_ID from TBLS where TBL_NAME = '"+tableName.toLowerCase()+"'");
        int tblid = -1;
        if(rs.next()) {
            tblid = rs.getInt(1);
        }
        rs.close();
        return tblid;
    }

    public static boolean tableExists(String tableName) throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();

        createDbTableIfNotExists(stmt);

        try {
            ResultSet rs = stmt.executeQuery("select count(*) from AXSTABLES where TBL_NAME = '"+tableName.toLowerCase()+"'");
            rs.next();
            int cnt = rs.getInt(1);
            rs.close();
            return cnt > 0;
        } finally {
            try { stmt.close(); } catch(Exception ee) { }
            try { conn.close(); } catch(Exception ee) { }
        }
    }

    public static List<AxsTableDef> listTables() throws Exception {
        ArrayList<AxsTableDef> res = new ArrayList<AxsTableDef>();
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();

        createDbTableIfNotExists(stmt);

        try {
            ResultSet rs = stmt.executeQuery("select * from AXSTABLES");
            while(rs.next()) {
                String lccols = rs.getString(9);
                String[] lcColumns = null;
                if(lccols != null)
                    lcColumns = lccols.split(",");
                AxsTableDef def = new AxsTableDef(
                        rs.getInt(1),
                        rs.getString(2),
                        rs.getInt(3),
                        rs.getDouble(4),
                        rs.getString(5),
                        rs.getString(6),
                        rs.getString(7),
                        rs.getInt(8) > 0,
                        lcColumns
                );
                res.add(def);
            }
            rs.close();
        } finally {
            try { stmt.close(); } catch(Exception ee) { }
            try { conn.close(); } catch(Exception ee) { }
        }
        return res;
    }

    public static boolean saveNewTable(String tableName, int numBuckets, double zoneHeight, String bucketCol, String raCol, String decCol,
                                    boolean lightcurves, String[] lcColumns) throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();

        createDbTableIfNotExists(stmt);

        int tblId = getSparkTableId(tableName, stmt);
        if(tblId == -1)
            throw new Exception("Spark table "+tableName+" not found!");
        try {
            return stmt.executeUpdate("insert into AXSTABLES (TBL_ID, TBL_NAME, NUM_BUCKETS, ZONE_HEIGHT, BUCKET_COL, RA_COL, DEC_COL, LC, LC_COLS) " +
                    "values (" + tblId + ", '" + tableName.toLowerCase() + "', " + numBuckets + ", " + zoneHeight + ", '" + bucketCol + "', '" + raCol + "', '" + decCol + "', " +
                    (lightcurves ? 1 : 0) + ", '" + lcColumns + "')") > 0;
        } finally {
            try { stmt.close(); } catch(Exception ee) { }
            try { conn.close(); } catch(Exception ee) { }
        }
    }

    public static boolean renameTable(String tableName, String newName) throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();

        createDbTableIfNotExists(stmt);

        String tableNameLower = tableName.toLowerCase();
        String newNameLow = newName.toLowerCase();

        int tblId = getSparkTableId(tableNameLower, stmt);

        if(tblId == -1)
            throw new Exception("Spark table "+tableName+" not found!");
        try {
            return stmt.executeUpdate("update AXSTABLES set TBL_NAME = '"+newNameLow+"' where TBL_ID = "+tblId) > 0;
        } finally {
            try { stmt.close(); } catch(Exception ee) { }
            try { conn.close(); } catch(Exception ee) { }
        }
    }

    public static boolean deleteTable(String tableName) throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();

        createDbTableIfNotExists(stmt);

        int tblId = getSparkTableId(tableName.toLowerCase(), stmt);
        try {
            if(tblId == -1)
            {
                System.err.println("Spark table "+tableName+" ID not found. Deleting by name.");
                return stmt.executeUpdate("delete from AXSTABLES where TBL_NAME = '"+tableName.toLowerCase()+"'") > 0;
            }
            return stmt.executeUpdate("delete from AXSTABLES where TBL_ID = "+tblId) > 0;
        } finally {
            try { stmt.close(); } catch(Exception ee) { }
            try { conn.close(); } catch(Exception ee) { }
        }
    }

    static class AxsTableDef {
        public AxsTableDef(int tableId, String tableName, int numBuckets, Double zoneHeight, String bucketCol, String raCol, String decCol, boolean lightcurves, String[] lcColumns) {
            this.tableId = tableId;
            this.tableName = tableName;
            this.numBuckets = numBuckets;
            this.zoneHeight = zoneHeight;
            this.bucketCol = bucketCol;
            this.raCol = raCol;
            this.decCol = decCol;
            this.lightcurves = lightcurves;
            this.lcColumns = lcColumns;
        }
        public int tableId;
        public String tableName;
        public int numBuckets;
        public Double zoneHeight;
        public String bucketCol;
        public String raCol;
        public String decCol;
        public boolean lightcurves;
        public String[] lcColumns;

        public String getTableId() {
            return tableName;
        }

        public String getTableName() {
            return tableName;
        }

        public int getNumBuckets() {
            return numBuckets;
        }

        public Double getZoneHeight() { return zoneHeight; }

        public String getBucketCol() {
            return bucketCol;
        }

        public String getRaCol() {
            return raCol;
        }

        public String getDecCol() {
            return decCol;
        }

        public boolean isLightcurves() {
            return lightcurves;
        }

        public String[] getLcColumns() {
            return lcColumns;
        }
    }

}
