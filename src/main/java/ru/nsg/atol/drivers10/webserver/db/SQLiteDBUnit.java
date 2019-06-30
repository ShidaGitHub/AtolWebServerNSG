package ru.nsg.atol.drivers10.webserver.db;

import org.sqlite.SQLiteErrorCode;
import ru.atol.drivers10.webserver.db.DBException;
import ru.atol.drivers10.webserver.db.NotUniqueKeyException;
import ru.atol.drivers10.webserver.db.SQLiteDB;
import ru.atol.drivers10.webserver.entities.BlockRecord;
import ru.atol.drivers10.webserver.entities.SubtaskStatus;
import ru.atol.drivers10.webserver.entities.Task;
import ru.atol.drivers10.webserver.entities.TasksStat;
import ru.nsg.atol.drivers10.webserver.entities.BlockRecordUnit;
import ru.nsg.atol.drivers10.webserver.entities.SubtaskStatusUnit;
import ru.nsg.atol.drivers10.webserver.entities.TaskUnit;

import java.io.File;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

public class SQLiteDBUnit extends SQLiteDB {
    private static final int CURRENT_VERSION = 2;
    
    public SQLiteDBUnit(){
        super();
    }

    private Connection connect() throws DBException {
        try {
            Class.forName("org.sqlite.JDBC");
            File dir = new File(System.getProperty("db.directory"));
            if (!dir.exists()) {
                dir.mkdirs();
            }

            Connection connection = DriverManager.getConnection(String.format("jdbc:sqlite:%s/web.db", dir.getAbsolutePath()));
            return connection;
        } catch (Exception var3) {
            throw new DBException(var3);
        }
    }

    private void closeConnection(Connection c) {
        try {
            if (c != null) {
                c.close();
            }
        } catch (SQLException var3) {
        }

    }

    private void rollback(Connection c) {
        try {
            if (c != null) {
                c.rollback();
            }
        } catch (SQLException var3) {
        }

    }

    private void updateDBVersion(int version, Connection connection) throws SQLException {
        PreparedStatement pstmt = connection.prepareStatement("UPDATE meta SET value = ? WHERE key = 'version'");
        pstmt.setString(1, String.valueOf(version));
        pstmt.executeUpdate();
    }

    private int convertBD(int fromVersion, Connection connection) throws SQLException {
        Statement stmt = connection.createStatement();
        switch(fromVersion) {
            case 1:
                stmt.execute("ALTER TABLE json_tasks ADD is_canceled BOOLEAN DEFAULT 0");
                this.updateDBVersion(2, connection);
            default:
                return 2;
        }
    }

    @Override
    public synchronized void init() throws DBException {
        Connection connection = null;
        try {
            connection = connect();
            Statement stmt = connection.createStatement();
            stmt.execute("CREATE TABLE IF NOT EXISTS meta([key] VARCHAR (10) PRIMARY KEY NOT NULL, value TEXT);");
            ResultSet versionQueryResult = stmt.executeQuery("SELECT value FROM meta WHERE key = 'version';");

            String version;
            for(version = ""; versionQueryResult.next(); version = versionQueryResult.getString("value")) {}
            PreparedStatement pstmt;
            if (version.isEmpty()) {
                version = String.valueOf(2);
                pstmt = connection.prepareStatement("INSERT INTO meta (key, value) VALUES (?, ?)");
                pstmt.setString(1, "version");
                pstmt.setString(2, version);
                pstmt.executeUpdate();
            }
            if (!version.isEmpty() && Integer.parseInt(version) < 2) {
                version = String.valueOf(this.convertBD(Integer.parseInt(version), connection));
            }
            if (Integer.parseInt(version) != 2) {
                throw new DBException("Неверная версия БД");
            }

            //json_tasks
            stmt.execute("CREATE TABLE IF NOT EXISTS json_tasks(" +
                    "uuid VARCHAR (36) PRIMARY KEY NOT NULL, " +
                    "kkm_unit TEXT NOT NULL, " +
                    "data TEXT, " +
                    "is_ready BOOLEAN DEFAULT 0, " +
                    "is_canceled BOOLEAN DEFAULT 0, " +
                    "timestamp DATETIME);");
            stmt.execute("CREATE INDEX IF NOT EXISTS kkm_unit_index on json_tasks (kkm_unit);");

            //json_results
            stmt.execute("CREATE TABLE IF NOT EXISTS json_results (" +
                    "uuid VARCHAR (36) NOT NULL, " +
                    "number INTEGER NOT NULL, " +
                    "timestamp DATETIME, " +
                    "status INTEGER DEFAULT 0, " +
                    "error_code INTEGER DEFAULT 0, " +
                    "error_description TEXT DEFAULT '', " +
                    "result_data TEXT DEFAULT '', " +
                    "send_code INTEGER DEFAULT 0, " +
                    "send_res TEXT DEFAULT '', " +
                    "PRIMARY KEY (uuid, number));");

            try {
                stmt.execute("CREATE TABLE settings (" +
                        "setting VARCHAR (32) PRIMARY KEY NOT NULL, " +
                        "value TEXT);");
                pstmt = connection.prepareStatement("INSERT INTO settings (setting, value) VALUES (?, ?)");
                pstmt.setString(1, "clear_interval");
                pstmt.setString(2, "720");
                pstmt.executeUpdate();
            } catch (Exception var12) {}

            try {
                stmt.execute("CREATE TABLE state (state_id  VARCHAR (32) PRIMARY KEY NOT NULL, " +
                        "kkm_unit TEXT NOT NULL, " +
                        "value TEXT);");
            } catch (Exception var11) {}

            stmt.execute("CREATE TRIGGER IF NOT EXISTS clear_tasks " +
                    "BEFORE INSERT ON json_tasks BEGIN DELETE FROM json_tasks " +
                    "WHERE (JULIANDAY('now', 'localtime') - JULIANDAY(timestamp)) * 24 >= " +
                        "(SELECT CAST(value as INTEGER) FROM settings WHERE setting = 'clear_interval'); " +
                    "END;");
            stmt.execute("CREATE TRIGGER IF NOT EXISTS clear_results " +
                    "BEFORE INSERT ON json_results BEGIN DELETE FROM json_results " +
                    "WHERE ((JULIANDAY('now', 'localtime') - JULIANDAY(timestamp)) * 24 >= " +
                        "(SELECT CAST(value as INTEGER) FROM settings WHERE setting = 'clear_interval')); " +
                    "END;");

            stmt.execute("PRAGMA journal_mode = WAL;");

        } catch (Exception var13) {
            throw new DBException(var13);
        } finally {
            closeConnection(connection);
        }
    }

    public synchronized void addTaskUnit(TaskUnit taskUnit) throws DBException {
        Connection connection = null;

        try {
            connection = this.connect();
            connection.setAutoCommit(false);
            PreparedStatement pstmt = connection.prepareStatement("INSERT INTO json_tasks (uuid, data, kkm_unit, is_ready, timestamp) VALUES (?, ?, ?, 0, ?)");
            pstmt.setString(1, taskUnit.getUuid());
            pstmt.setString(2, taskUnit.getData());
            pstmt.setString(3, taskUnit.getUnit());
            pstmt.setObject(4, taskUnit.getTimestamp().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());
            pstmt.executeUpdate();

            for(int i = 0; i < taskUnit.getSubTaskCount(); ++i) {
                pstmt = connection.prepareStatement("INSERT INTO json_results (uuid, number, timestamp) VALUES (?, ?, ?)");
                pstmt.setString(1, taskUnit.getUuid());
                pstmt.setInt(2, i);
                pstmt.setObject(3, taskUnit.getTimestamp().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());
                pstmt.executeUpdate();
            }
            connection.commit();
        } catch (SQLException var8) {
            rollback(connection);
            if (var8.getErrorCode() == SQLiteErrorCode.SQLITE_CONSTRAINT.code) {
                throw new NotUniqueKeyException(var8);
            } else {
                throw new DBException(var8);
            }
        } finally {
            this.closeConnection(connection);
        }
    }

    public synchronized void addSubTaskUnitOnly(SubtaskStatusUnit subTaskUnit) throws DBException {
        Connection connection = null;

        try {
            connection = this.connect();
            connection.setAutoCommit(false);
            PreparedStatement pstmt = connection.prepareStatement("INSERT INTO json_results (uuid, number, timestamp, " +
                                                                                                 "status, error_code, error_description, " +
                                                                                                 "result_data, send_code, send_res) " +
                                                                                                 "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
            pstmt.setString(1, subTaskUnit.getUuid());
            pstmt.setInt(2, subTaskUnit.getNumber());
            pstmt.setObject(3, subTaskUnit.getTimestamp().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());
            pstmt.setInt(4, subTaskUnit.getStatus());
            pstmt.setInt(5, subTaskUnit.getErrorCode());
            pstmt.setString(6, subTaskUnit.getErrorDescription());
            pstmt.setString(7, subTaskUnit.getResultData());
            pstmt.setInt(8, subTaskUnit.getSendCode());
            pstmt.setString(9, subTaskUnit.getSendRes());

            pstmt.executeUpdate();
            connection.commit();
        } catch (SQLException var8) {
            rollback(connection);
            if (var8.getErrorCode() == SQLiteErrorCode.SQLITE_CONSTRAINT.code) {
                throw new NotUniqueKeyException(var8);
            } else {
                throw new DBException(var8);
            }
        } finally {
            this.closeConnection(connection);
        }
    }

    public synchronized TaskUnit getNextTaskUnit(String unit) throws DBException {
        Connection connection = null;

        TaskUnit var5;
        try {
            connection = this.connect();
            PreparedStatement pstmt = connection.prepareStatement("SELECT uuid, data, MIN(timestamp) FROM json_tasks " +
                    "WHERE kkm_unit = ? AND is_ready != 1 AND is_canceled != 1");
            pstmt.setString(1, unit.trim());
            ResultSet result = pstmt.executeQuery();

            TaskUnit taskUnit = new TaskUnit();
            while(result.next()) {
                taskUnit.setUuid(result.getString("uuid"));
                taskUnit.setUnit(unit);
                taskUnit.setData(result.getString("data"));
            }
            if (taskUnit.getUuid() == null) {
                var5 = null;
                return var5;
            }
            var5 = taskUnit;
        } catch (Exception var9) {
            throw new DBException(var9);
        } finally {
            this.closeConnection(connection);
        }
        return var5;
    }

    public TaskUnit getTaskUnit(String uuid) throws DBException {
        Connection connection = null;

        TaskUnit var6;
        try {
            connection = this.connect();
            PreparedStatement pstmt = connection.prepareStatement("SELECT uuid, kkm_unit, data, is_ready, is_canceled FROM json_tasks WHERE uuid == ?");
            pstmt.setString(1, uuid);
            ResultSet result = pstmt.executeQuery();
            TaskUnit taskUnit = new TaskUnit();

            while(result.next()) {
                taskUnit.setUuid(result.getString("uuid"));
                taskUnit.setUnit(result.getString("kkm_unit"));
                taskUnit.setData(result.getString("data"));
                taskUnit.setReady(result.getBoolean("is_ready"));
                taskUnit.setCanceled(result.getBoolean("is_canceled"));
            }

            if (taskUnit.getUuid() != null) {
                var6 = taskUnit;
                return var6;
            }

            var6 = null;
        } catch (Exception var10) {
            throw new DBException(var10);
        } finally {
            this.closeConnection(connection);
        }

        return var6;
    }

    public synchronized void setTaskUnitReady(String uuid) throws DBException {
        Connection connection = null;

        try {
            connection = this.connect();
            PreparedStatement pstmt = connection.prepareStatement("UPDATE json_tasks SET is_ready = ? WHERE uuid = ?");
            pstmt.setBoolean(1, true);
            pstmt.setString(2, uuid);
            pstmt.executeUpdate();
        } catch (Exception var7) {
            throw new DBException(var7);
        } finally {
            this.closeConnection(connection);
        }

    }

    public synchronized List<SubtaskStatus> getTaskUnitStatus(String uuid) throws DBException {
        Connection connection = null;
        try {
            connection = this.connect();
            PreparedStatement pstmt = connection.prepareStatement("SELECT status, error_code, error_description, result_data FROM json_results WHERE uuid = ? ORDER BY number");
            pstmt.setString(1, uuid);
            ResultSet result = pstmt.executeQuery();
            LinkedList status = new LinkedList();

            while(result.next()) {
                SubtaskStatus s = new SubtaskStatus();
                s.setStatus(result.getInt("status"));
                s.setErrorCode(result.getInt("error_code"));
                s.setErrorDescription(result.getString("error_description"));
                s.setResultData(result.getString("result_data"));
                status.add(s);
            }
            return status;
        } catch (Exception var10) {
            throw new DBException(var10);
        } finally {
            this.closeConnection(connection);
        }
    }

    @Override
    public synchronized void updateSubTaskStatus(String uuid, int number, SubtaskStatus status) throws DBException {
        Connection connection = null;
        try {
            connection = this.connect();
            PreparedStatement pstmt = connection.prepareStatement("UPDATE json_results SET  status = ?, error_code = ?, error_description = ?, result_data = ? WHERE uuid = ? AND number = ?");
            pstmt.setInt(1, status.getStatus());
            pstmt.setInt(2, status.getErrorCode());
            pstmt.setString(3, status.getErrorDescription());
            pstmt.setString(4, status.getResultData());
            pstmt.setString(5, uuid);
            pstmt.setInt(6, number);
            pstmt.executeUpdate();
        } catch (Exception var9) {
            throw new DBException(var9);
        } finally {
            this.closeConnection(connection);
        }
    }

    public void blockDBU(BlockRecordUnit blockUnit) throws DBException {
        Connection connection = null;

        try {
            connection = this.connect();
            PreparedStatement pstmt = connection.prepareStatement("INSERT OR REPLACE INTO state (state_id, kkm_unit, value) VALUES (?, ?, ?)");
            pstmt.setString(1, "block_uuid");
            pstmt.setString(2, blockUnit.getUnit());
            pstmt.setString(3, blockUnit.getUuid());
            pstmt.executeUpdate();

            pstmt = connection.prepareStatement("INSERT OR REPLACE INTO state (state_id, kkm_unit, value) VALUES (?, ?, ?)");
            pstmt.setString(1, "block_last_fd");
            pstmt.setString(2, blockUnit.getUnit());
            pstmt.setString(3, String.valueOf(blockUnit.getDocumentNumber()));
            pstmt.executeUpdate();
        } catch (Exception var7) {
            throw new DBException(var7);
        } finally {
            this.closeConnection(connection);
        }

    }

    public BlockRecordUnit getBlockUnitState(String unit) throws DBException {
        Connection connection = null;

        try {
            connection = this.connect();

            PreparedStatement pstmt = connection.prepareStatement("SELECT state_id, value FROM state WHERE kkm_unit == ?");
            pstmt.setString(1, unit);
            ResultSet result = pstmt.executeQuery();
            String blockUUID = "";
            String blockDocument = "-1";

            while(result.next()) {
                String key = result.getString("state_id");
                String value = result.getString("value");
                byte var9 = -1;
                switch(key.hashCode()) {
                    case 386293077:
                        if (key.equals("block_last_fd")) {
                            var9 = 1;
                        }
                        break;
                    case 1286584365:
                        if (key.equals("block_uuid")) {
                            var9 = 0;
                        }
                }

                switch(var9) {
                    case 0:
                        blockUUID = value;
                        break;
                    case 1:
                        blockDocument = value;
                }
            }

            BlockRecordUnit var15 = new BlockRecordUnit(blockUUID, Long.parseLong(blockDocument), unit);
            return var15;
        } catch (Exception var13) {
            throw new DBException(var13);
        } finally {
            this.closeConnection(connection);
        }
    }

    public void unblockDBUnit(String unit) throws DBException {
        Connection connection = null;

        try {
            connection = this.connect();
            PreparedStatement pstmt = connection.prepareStatement("DELETE FROM state WHERE state_id = ? AND kkm_unit = ?");
            pstmt.setString(1, "block_uuid");
            pstmt.setString(2, unit);
            pstmt.execute();

            pstmt = connection.prepareStatement("DELETE FROM state WHERE state_id = ? AND kkm_unit = ?");
            pstmt.setString(1, "block_last_fd");
            pstmt.setString(2, unit);
            pstmt.execute();
        } catch (Exception var6) {
            throw new DBException(var6);
        } finally {
            this.closeConnection(connection);
        }

    }

    public TasksStat getTasksStatOnUnit(String unit) throws DBException {
        TasksStat stat = new TasksStat();
        Connection connection = null;

        try {
            connection = this.connect();
            PreparedStatement pstmt = connection.prepareStatement("SELECT COUNT(*) FROM json_tasks WHERE is_canceled = 1 AND kkm_unit = ?");
            pstmt.setString(1, unit);
            ResultSet result = pstmt.executeQuery();
            int canceled = 0;
            while(result.next()) {
                canceled = result.getInt(1);
                stat.setTasksCanceledCount(canceled);
            }

            pstmt = connection.prepareStatement("SELECT is_ready, COUNT(*) FROM json_tasks GROUP BY is_ready AND kkm_unit = ?");
            pstmt.setString(1, unit);
            result = pstmt.executeQuery();
            while(result.next()) {
                if (result.getBoolean(1)) {
                    stat.setTasksReadyCount(result.getInt(2) - canceled);
                } else {
                    stat.setTasksNotReadyCount(result.getInt(2));
                }
            }

            TasksStat var6 = stat;
            return var6;
        } catch (Exception var10) {
            throw new DBException(var10);
        } finally {
            this.closeConnection(connection);
        }
    }

    public void cancelTaskUnit(String uuid) throws DBException {
        Connection connection = null;

        try {
            connection = this.connect();
            PreparedStatement pstmt = connection.prepareStatement("UPDATE json_tasks SET is_ready = ?, is_canceled = ? WHERE uuid = ?");
            pstmt.setBoolean(1, true);
            pstmt.setBoolean(2, true);
            pstmt.setString(3, uuid);
            pstmt.executeUpdate();
        } catch (Exception var7) {
            throw new DBException(var7);
        } finally {
            this.closeConnection(connection);
        }

    }

    public synchronized List<SubtaskStatusUnit> getNextSubTaskListToSend() throws DBException {
        Connection connection = null;
        LinkedList<SubtaskStatusUnit> subTaskList = new LinkedList<>();
        try {
            connection = this.connect();
            PreparedStatement pstmt = connection.prepareStatement("SELECT uuid, number, status, error_code, " +
                                                                       "error_description, result_data, timestamp AS timestamp " +
                                                                       "FROM json_results " +
                                                                       "WHERE send_code = 0 AND status > 1 LIMIT 500");
            ResultSet result = pstmt.executeQuery();
            while(result.next()) {
                SubtaskStatusUnit subTask = new SubtaskStatusUnit();
                subTask.setUuid(result.getString("uuid"));
                if (subTask.getUuid() == null) continue;
                subTask.setNumber(result.getInt("number"));

                String timestamp = result.getString("timestamp");
                if (timestamp != null && timestamp.length() == 23)
                    subTask.setTimestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(timestamp.replaceAll("T", " ")));
                else
                    subTask.setTimestamp(new Date((Calendar.getInstance().getTime()).getTime()));
                subTask.setStatus(result.getInt("status"));
                subTask.setErrorCode(result.getInt("error_code"));
                subTask.setErrorDescription(result.getString("error_description"));
                subTask.setResultData(result.getString("result_data"));

                subTaskList.add(subTask);
            }
            if (subTaskList.isEmpty()) return null;
        } catch (Exception var9) {
            throw new DBException(var9);
        } finally {
            this.closeConnection(connection);
        }
        return subTaskList;
    }

    public synchronized void updateSubTaskSendStatus(List<SubtaskStatusUnit> subTaskList) throws DBException {
        try (Connection connection = this.connect()){
            connection.setAutoCommit(false);
            PreparedStatement pstmt = connection.prepareStatement("UPDATE json_results SET  send_code = ?, send_res = ? WHERE uuid = ? AND number = ?");

            for (SubtaskStatusUnit subTask : subTaskList){
                pstmt.setInt(1, subTask.getSendCode());
                pstmt.setString(2, subTask.getSendRes());
                pstmt.setString(3, subTask.getUuid());
                pstmt.setInt(4, subTask.getNumber());
                pstmt.addBatch();
            }
            pstmt.executeBatch();

            connection.commit();
        } catch (Exception var9) {
            throw new DBException(var9);
        }
    }


    //Переопределенные зарезать
    @Override
    public synchronized void addTask(Task task){
        throw new RuntimeException("addTask not support, use addTaskUnit");
    }

    @Override
    public synchronized Task getNextTask(){
        throw new RuntimeException("getNextTask not support, use getNextTaskUnit");
    }

    @Override
    public Task getTask(String uuid) throws DBException{
        throw new RuntimeException("getTask not support, use getTaskUnit");
    }

    @Override
    public synchronized void setTaskReady(String uuid) throws DBException{
        throw new RuntimeException("setTaskReady not support, use setTaskUnitReady");
    }

    @Override
    public synchronized List<SubtaskStatus> getTaskStatus(String uuid) throws DBException{
        throw new RuntimeException("getTaskStatus not support, use getTaskUnitStatus");
    }

    @Override
    public void blockDB(BlockRecord block) throws DBException{
        throw new RuntimeException("blockDB not support, use blockDBU");
    }

    @Override
    public BlockRecord getBlockState() throws DBException{
        throw new RuntimeException("getBlockState not support, use getBlockUnitState");
    }

    @Override
    public void unblockDB() throws DBException{
        throw new RuntimeException("unblockDB not support, use unblockDBUnit");
    }

    @Override
    public TasksStat getTasksStat() throws DBException{
        throw new RuntimeException("getTasksStat not support, use getTasksStatOnUnit");
    }

    @Override
    public void cancelTask(String uuid) throws DBException{
        throw new RuntimeException("cancelTask not support, use cancelTaskUnit");
    }
}
