using Oracle.DataAccess.Client;
using System;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace OracleBackup
{
    class OracleManager
    {
        private const string ALLOW_CLEAR_TABLE_AFTER_BACKUP = "AllowClearTableAfterBackup";
        private readonly static string allowClearTableAfterBackup = ConfigurationManager.AppSettings[ALLOW_CLEAR_TABLE_AFTER_BACKUP];
        private bool AllowClearTableAfterBackup { get { return allowClearTableAfterBackup == "true"; } }
        public bool CheckOracle(Config config)
        {
            Log.log.Debug("开始检查Oracle。");
            var oracles = config.BackupOracles;
            foreach (var oracle in oracles)
            {
                string database = oracle.OracleDatabase;
                string username = oracle.OracleUsername;
                string password = oracle.OraclePassword;
                string connStr = GetConnectionString(database, username, password);
                if (string.IsNullOrEmpty(connStr))
                {
                    Log.log.Info("Oracle连接配置错误。");
                    return false;
                }
                Log.log.Debug("检查Oracle能否连接。");
                if (!CheckOracleConnection(connStr))
                {
                    Log.log.Info("无法连接到Oracle，[" + username + "/" + password + "@" + database + "]。");
                }
                if (!oracle.FullBackup)
                {
                    var tables = oracle.BackupTables;
                    Log.log.Debug("检查表是否存在。");
                    foreach (var table in tables)
                    {
                        var tablename = table.Tablename;
                        if (!CheckTableExist(connStr, tablename))
                        {
                            Log.log.Info("表[" + tablename + "]不存在，Oracle，[" + username + "/" + password + "@" + database + "]。");
                        }
                    }
                }
            }
            Log.log.Debug("完成检查Oracle。");
            return true;
        }
        public string GetConnectionString(string db, string username, string password)
        {
            db = "tcp://" + db;
            Uri u = new Uri(db);
            string ip = u.Host;
            string port = u.Port.ToString();
            string d = u.AbsolutePath?.Replace("/", "");
            if (string.IsNullOrEmpty(ip) || string.IsNullOrEmpty(port) || string.IsNullOrEmpty(d))
            {
                return null;
            }
            return "Data Source = (DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = " + ip + ")(PORT = " + port + "))(CONNECT_DATA = (SERVICE_NAME = " + d + "))); User Id = " + username + "; Password = " + password + ";";
        }
        private bool CheckTableExist(string connStr, string tablename)
        {
            string sql = "select count(*) from user_tables where table_name = '" + tablename.ToUpper() + "'";
            DataSet dataSet = Query(connStr, sql);
            if (dataSet == null || dataSet.Tables.Count <= 0)
            {
                return false;
            }
            DataTable dt = dataSet.Tables[0];
            object ov = dt.Rows[0][0];
            if (ov == null || ov == DBNull.Value)
            {
                return false;
            }
            int count = 0;
            if (int.TryParse(ov.ToString(), out count))
            {
                if (count > 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }
        private bool CheckOracleConnection(string connStr)
        {
            bool canOpen = false;
            using (OracleConnection connection = new OracleConnection(connStr))
            {
                connection.Open();
                canOpen = true;
            }
            return canOpen;
        }
        private DataSet Query(string connStr, string cmdText)
        {
            DataSet ds = null;
            using (var connection = new OracleConnection(connStr))
            {
                connection.Open();
                using (var cmd = new OracleCommand())
                {
                    cmd.Connection = connection;
                    cmd.CommandText = cmdText;
                    using (var adapter = new OracleDataAdapter(cmd))
                    {
                        ds = new DataSet();
                        adapter.Fill(ds);
                    }
                }
            }
            return ds;
        }
        private int Execute(string connStr, string cmdText)
        {
            int count = 0;
            using (var connection = new OracleConnection(connStr))
            {
                connection.Open();
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        using (var cmd = new OracleCommand())
                        {
                            cmd.Connection = connection;
                            cmd.CommandText = cmdText;
                            count = cmd.ExecuteNonQuery();
                        }
                        transaction.Commit();
                    }
                    catch
                    {
                        transaction.Rollback();
                        count = 0;
                    }
                }
            }
            return count;
        }
        public void Backup(Config config)
        {
            if (!CheckOracle(config)) { return; }
            Thread.Sleep(1000);
            GC.Collect();
            Log.log.Info("开始备份。");
            var oracles = config.BackupOracles;
            foreach (var oracle in oracles)
            {
                if (oracle.FullBackup) { FullBackup(oracle.OracleUsername, oracle.OraclePassword, oracle.OracleDatabase, oracle.BackupDirectoryPath); }
                else { TableBackup(oracle.OracleUsername, oracle.OraclePassword, oracle.OracleDatabase, oracle.BackupDirectoryPath, oracle.BackupTables); }
                Thread.Sleep(1000);
                GC.Collect();
            }
            Log.log.Info("结束备份。");
        }
        private void FullBackup(string username, string password, string database, string directory)
        {
            Log.log.Info("开始完全备份[" + username + "/" + password + "@" + database + "]。");
            var now = DateTime.Now;
            string strDate = now.Year.ToString("0000") + now.Month.ToString("00") + now.Day.ToString("00") + now.Hour.ToString("00") + now.Minute.ToString("00") + now.Second.ToString("00");
            string filename = "OracleFullBackup_" + strDate;
            string dmpFilename = filename + ".dmp";
            string logFilename = filename + "_Log.txt";
            CheckDirectoryExistOrCreateDirectory(directory);
            Log.log.Debug("Directory[" + directory + "]。");
            string dmpFilePath = directory + "/" + dmpFilename;
            CheckFileExistOrCreateFile(dmpFilePath);
            Log.log.Debug("DMP File[" + dmpFilePath + "]。");
            Log.log.Info("备份文件[" + dmpFilePath + "]。");
            string logFilePath = directory + "/" + logFilename;
            CheckFileExistOrCreateFile(logFilePath);
            Log.log.Debug("Log File[" + logFilePath + "]。");
            string loginString = BuildOracleLoginString(username, password, database);
            string cmd = "EXP " + loginString + " full=y file='" + dmpFilePath + "' log='" + logFilePath + "'";
            Log.log.Debug("Command[" + cmd + "]。");
            using (var process = new Process())
            {
                process.StartInfo.FileName = "cmd.exe";
                process.StartInfo.CreateNoWindow = true;
                process.StartInfo.UseShellExecute = false;
                process.StartInfo.RedirectStandardInput = true;
                process.Start();
                process.StandardInput.WriteLine(cmd);
                process.StandardInput.WriteLine("exit");
                process.WaitForExit();
            }
            Log.log.Info("结束完全备份[" + username + "/" + password + "@" + database + "]。");
        }
        private void TableBackup(string username, string password, string database, string directory, BackupTable[] tables)
        {
            Log.log.Info("开始表备份[" + username + "/" + password + "@" + database + "]。");
            var now = DateTime.Now;
            string strDate = now.Year.ToString("0000") + now.Month.ToString("00") + now.Day.ToString("00") + now.Hour.ToString("00") + now.Minute.ToString("00") + now.Second.ToString("00");
            string filename = "OracleTableBackup_" + strDate;
            string dmpFilename = filename + ".dmp";
            string logFilename = filename + "_Log.txt";
            CheckDirectoryExistOrCreateDirectory(directory);
            Log.log.Debug("Directory[" + directory + "]。");
            string dmpFilePath = directory + "/" + dmpFilename;
            CheckFileExistOrCreateFile(dmpFilePath);
            Log.log.Debug("DMP File[" + dmpFilePath + "]。");
            Log.log.Info("备份文件[" + dmpFilePath + "]。");
            string logFilePath = directory + "/" + logFilename;
            CheckFileExistOrCreateFile(logFilePath);
            Log.log.Debug("Log File[" + logFilePath + "]。");
            string loginString = BuildOracleLoginString(username, password, database);
            string tablenames = "";
            foreach (var table in tables)
            {
                if (string.IsNullOrEmpty(tablenames)) { tablenames += table.Tablename; }
                else { tablenames += "," + table.Tablename; }
            }
            string cmd = "EXP userid=" + loginString + " tables=(" + tablenames + ") file='" + dmpFilePath + "' log='" + logFilePath + "'";
            Log.log.Debug("Command[" + cmd + "]。");
            using (var process = new Process())
            {
                process.StartInfo.FileName = "cmd.exe";
                process.StartInfo.CreateNoWindow = true;
                process.StartInfo.UseShellExecute = false;
                process.StartInfo.RedirectStandardInput = true;
                process.Start();
                process.StandardInput.WriteLine(cmd);
                process.StandardInput.WriteLine("exit");
                process.WaitForExit();
            }
            Log.log.Info("结束表备份[" + username + "/" + password + "@" + database + "]。");
            Thread.Sleep(1000);
            if (AllowClearTableAfterBackup)
            {
                Log.log.Debug("Global allowed clear table after backup。");
                ClearTables(username, password, database, tables);
            }
            else { Log.log.Debug("Global not allowed clear table after backup。"); }
        }
        private void ClearTables(string username, string password, string database, BackupTable[] tables)
        {
            Log.log.Info("开始清空表。");
            foreach (var table in tables)
            {
                if (table.ClearTableAfterBackup)
                {
                    string name = table.Tablename;
                    Log.log.Info("开始清空表[" + name + "]。");
                    string connStr = GetConnectionString(database, username, password);
                    string sql = "delete from " + name;
                    int count = Execute(connStr, sql);
                    Log.log.Info("完成清空表[" + name + "]，共" + count + "行。");
                }
            }
            Log.log.Info("结束清空表。");
        }
        private string BuildOracleLoginString(string username, string password, string database)
        {
            if (username.ToUpper() == "SYS")
            {
                return username + "/" + password + "@" + database + " as sysdba";
            }
            else
            {
                return username + "/" + password + "@" + database;
            }
        }
        private void CheckFileExistOrCreateFile(string filePath)
        {
            if (!File.Exists(filePath))
            {
                File.Create(filePath).Dispose();
            }
        }
        private void CheckDirectoryExistOrCreateDirectory(string directory)
        {
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }
        }
    }
}
