using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Models
{

    public enum TaskResult
    {
        Success,
        Retry,
        Abort,
        Failed

    }
}
