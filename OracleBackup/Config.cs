namespace OracleBackup
{
    class Config
    {
        public BackupOracle[] BackupOracles { get; set; }
    }
    class BackupOracle
    {
        public string OracleDatabase { get; set; }
        public string OracleUsername { get; set; }
        public string OraclePassword { get; set; }
        public bool FullBackup { get; set; }
        public BackupTable[] BackupTables { get; set; }
        public string BackupDirectoryPath { get; set; }
    }
    class BackupTable {
        public string Tablename { get; set; }
        public bool ClearTableAfterBackup { get; set; }
    }
}
