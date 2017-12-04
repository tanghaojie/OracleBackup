using log4net;
using System.Reflection;

namespace OracleBackup
{
    class Log
    {
        public static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
    }
}
