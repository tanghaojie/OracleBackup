using Newtonsoft.Json;
using System.Configuration;
using System.IO;

namespace OracleBackup
{
    class ConfigManager
    {
        private const string CONFIG_FILE_PATH_APPSETTING_KEY = "ConfigFilePath";
        private readonly static string configFilePath = ConfigurationManager.AppSettings[CONFIG_FILE_PATH_APPSETTING_KEY];
        private static bool ConfigFileExist()
        {
            return File.Exists(configFilePath);
        }
        private static void CreateEmptyFile()
        {
            Config config = new Config
            {
                BackupOracles = new BackupOracle[] {
                     new BackupOracle{
                          OracleDatabase="",
                          OracleUsername="",
                          OraclePassword="",
                          FullBackup=true,
                          BackupTables=null,
                          BackupDirectoryPath="" },
                     new BackupOracle{
                          OracleDatabase="",
                          OracleUsername="",
                          OraclePassword="",
                          FullBackup=false,
                          BackupTables=new  BackupTable[]{
                              new BackupTable {
                                  Tablename = "Table1",
                                  ClearTableAfterBackup = false },
                              new BackupTable{
                                  Tablename = "Table2",
                                  ClearTableAfterBackup = true } },
                          BackupDirectoryPath=""
                     }
                 }
            };
            string json = JsonConvert.SerializeObject(config, new JsonSerializerSettings { Formatting = Formatting.Indented, StringEscapeHandling = StringEscapeHandling.EscapeNonAscii });
            TextStreamClass.Write(configFilePath, json);
        }
        public static Config Config()
        {
            if (!ConfigFileExist())
            {
                Log.log.Info("配置文件[" + configFilePath + "]不存在。创建新配置文件。");
                CreateEmptyFile();
                Log.log.Info("创建成功[" + configFilePath + "]。");
            }
            Log.log.Info("开始读取配置文件[" + configFilePath + "]。");
            string json = TextStreamClass.Read(configFilePath);
            Log.log.Debug("开始反序列化配置文件。");
            JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings { Formatting = Formatting.Indented, StringEscapeHandling = StringEscapeHandling.EscapeNonAscii };
            Config config = JsonConvert.DeserializeObject<Config>(json, jsonSerializerSettings);
            Log.log.Debug("完成反序列化配置文件。");
            Log.log.Info("完成读取配置文件[" + configFilePath + "]。");
            return config;
        }
        public static bool Check(Config config)
        {
            Log.log.Debug("开始检查配置文件。");
            if (config == null)
            {
                Log.log.Fatal("配置文件错误，尝试删除配置文件，然后重新运行程序并配置。");
                return false;
            }
            var backupOracles = config.BackupOracles;
            if (backupOracles == null)
            {
                Log.log.Fatal("配置文件错误，尝试删除配置文件，然后重新运行程序并配置。");
                return false;
            }
            var len = backupOracles.Length;
            if (len <= 0)
            {
                Log.log.Fatal("配置文件错误，尝试删除配置文件，然后重新运行程序并配置。");
                return false;
            }
            foreach (var backupOracle in backupOracles)
            {
                if (string.IsNullOrEmpty(backupOracle.OracleDatabase))
                {
                    Log.log.Fatal("需要配置Oracle数据库。");
                    return false;
                }
                if (string.IsNullOrEmpty(backupOracle.OracleUsername))
                {
                    Log.log.Fatal("需要配置Oracle用户名。");
                    return false;
                }
                if (string.IsNullOrEmpty(backupOracle.OraclePassword))
                {
                    Log.log.Fatal("需要配置Oracle密码。");
                    return false;
                }
                if (!backupOracle.FullBackup)
                {
                    var tables = backupOracle.BackupTables;
                    if (tables == null)
                    {
                        Log.log.Fatal("非完全备份时，必须设置备份得Oracle表。");
                        return false;
                    }
                    int length = tables.Length;
                    if (length <= 0)
                    {
                        Log.log.Fatal("非全部备份，必须设置备份得Oracle表。");
                        return false;
                    }
                    foreach (var table in tables)
                    {
                        if (string.IsNullOrEmpty(table.Tablename))
                        {
                            Log.log.Fatal("缺失表名。");
                            return false;
                        }
                    }
                }
                if (string.IsNullOrEmpty(backupOracle.BackupDirectoryPath))
                {
                    Log.log.Fatal("需要配置备份文件夹。");
                    return false;
                }
            }
            Log.log.Debug("完成检查配置文件。");
            return true;
        }
    }
}
