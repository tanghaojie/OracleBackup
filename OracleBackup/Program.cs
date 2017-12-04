using System;

namespace OracleBackup
{
    class Program
    {
        private static OracleManager om = new OracleManager();
        private const string CRASH_FILE_NAME = "Crash.txt";
        static void Main(string[] args)
        {
            try
            {
                Start();
                var config = PrepareConfig();
                om.Backup(config);
                End();
            }
            catch (Exception ex)
            {
                try
                {
                    Log.log.Fatal("Crashed.");
                    Log.log.Fatal("Message:" + ex.Message);
                    Log.log.Fatal("Data:" + ex.Data);
                    Log.log.Fatal("TargetSite:" + ex.TargetSite);
                    Log.log.Fatal("StackTrace:" + ex.StackTrace);
                    Log.log.Fatal("Source:" + ex.Source);
                }
                catch (Exception subex)
                {
                    TextStreamClass.Write(CRASH_FILE_NAME, "Crashed" + "\r\n");
                    var now = DateTime.Now;
                    TextStreamClass.Append(CRASH_FILE_NAME, now.ToShortDateString() + " " + now.ToShortTimeString() + "\r\n");
                    TextStreamClass.Append(CRASH_FILE_NAME, "------inner------" + "\r\n");
                    TextStreamClass.Append(CRASH_FILE_NAME, "Message:" + ex.Message + "\r\n");
                    TextStreamClass.Append(CRASH_FILE_NAME, "Data:" + ex.Data);
                    TextStreamClass.Append(CRASH_FILE_NAME, "TargetSite:" + ex.TargetSite + "\r\n");
                    TextStreamClass.Append(CRASH_FILE_NAME, "StackTrace:" + ex.StackTrace + "\r\n");
                    TextStreamClass.Append(CRASH_FILE_NAME, "Source:" + ex.Source + "\r\n");
                    TextStreamClass.Append(CRASH_FILE_NAME, "------outter------" + "\r\n");
                    TextStreamClass.Append(CRASH_FILE_NAME, "Message:" + subex.Message + "\r\n");
                    TextStreamClass.Append(CRASH_FILE_NAME, "Data:" + subex.Data + "\r\n");
                    TextStreamClass.Append(CRASH_FILE_NAME, "TargetSite:" + subex.TargetSite + "\r\n");
                    TextStreamClass.Append(CRASH_FILE_NAME, "StackTrace:" + subex.StackTrace + "\r\n");
                    TextStreamClass.Append(CRASH_FILE_NAME, "Source:" + subex.Source + "\r\n");
                }
            }
        }
        private static void Start()
        {
            Log.log.Info("");
            Log.log.Info("-- -- --");
            Log.log.Info("启动");
        }
        private static Config PrepareConfig()
        {
            Config config = ConfigManager.Config();
            if (!ConfigManager.Check(config))
            {
                End();
                Environment.Exit(0);
            }
            return config;
        }
        private static void End()
        {
            Log.log.Info("结束");
            Log.log.Info("-- -- --");
        }
    }
}
