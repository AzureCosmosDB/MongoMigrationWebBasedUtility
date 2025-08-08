using OnlineMongoMigrationProcessor.Models;
using System;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Helpers
{
    public class RetryHelper
    {
        public async Task<TaskResult> ExecuteTask(
            Func<Task<TaskResult>> taskFunc,
            Func<Exception, int, int, Task<TaskResult>> exceptionHandler,
            Log log,
            int maxTries=10,
            int initialDelayMs = 2000)
        {
            int attempt = 0;
            int delay = initialDelayMs;

            while (attempt < maxTries)
            {
                try
                {
                    var result = await taskFunc();
                    if (result != TaskResult.Retry)
                        return result;

                    attempt++;
                    log.WriteLine($"Retrying attempt {attempt} in {delay/1000} seconds...");
                    await Task.Delay(delay);
                    delay = delay * 2; // Exponential backoff

                }
                catch (Exception ex)
                {
                    attempt++;
                    int currentBackoffSeconds = delay / 1000;
                    var shouldRetry = await exceptionHandler(ex, attempt, currentBackoffSeconds);
                    if (shouldRetry==TaskResult.Abort || attempt >= maxTries)
                        return TaskResult.Abort;

                    await Task.Delay(delay);
                    delay = delay * 2; // Exponential backoff
                }
            }
            return TaskResult.FailedAfterRetries;
        }
    }

}
