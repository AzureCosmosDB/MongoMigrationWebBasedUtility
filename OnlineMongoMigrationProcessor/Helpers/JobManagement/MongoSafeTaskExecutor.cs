using MongoDB.Driver;
using System;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Helpers.JobManagement
{
    public static class MongoSafeTaskExecutor
    {
        public static async Task<T> ExecuteAsync<T>(
            Func<CancellationToken, Task<T>> operation,
            int timeoutSeconds,
            string operationName,
            Action<string>? logAction = null,
            CancellationToken? externalToken = null,
            MongoClient? clientToKill = null)
        {
            using var cts = externalToken.HasValue
                ? CancellationTokenSource.CreateLinkedTokenSource(externalToken.Value)
                : new CancellationTokenSource();

            var startTime = DateTime.UtcNow;
            var timeoutTask = Task.Delay(TimeSpan.FromSeconds(timeoutSeconds), CancellationToken.None);

            logAction?.Invoke($"[START] {operationName} | Timeout={timeoutSeconds}s | Start={startTime:O}");

            var workerTask = Task.Run(async () =>
            {
                try
                {
                    logAction?.Invoke($"[TASK-BEGIN] {operationName} started on ThreadId={Environment.CurrentManagedThreadId}");
                    var result = await operation(cts.Token);
                    logAction?.Invoke($"[TASK-END] {operationName} completed successfully.");
                    return result;
                }
                catch (OperationCanceledException)
                {
                    logAction?.Invoke($"[CANCELLED] {operationName} token cancelled.");
                    throw;
                }
                catch (Exception ex)
                {
                    logAction?.Invoke($"[EXCEPTION] {operationName} threw: {ex.GetType().Name} - Details: {ex}");
                    throw;
                }
            }, cts.Token);

            Task completedTask;
            try
            {
                completedTask = await Task.WhenAny(workerTask, timeoutTask);
            }
            catch (Exception ex)
            {
                logAction?.Invoke($"[WHENANY-ERROR] {operationName} race failed. Details: {ex}");
                throw;
            }

            // -----------------------------
            // TIMEOUT LOGIC
            // -----------------------------
            if (completedTask == timeoutTask)
            {
                logAction?.Invoke($"[TIMEOUT] {operationName} exceeded {timeoutSeconds}s. Cancelling token...");
                cts.Cancel();

                try
                {
                    logAction?.Invoke($"[CANCEL-ATTEMPT] Waiting 2s for {operationName} to stop...");
                    await Task.WhenAny(workerTask, Task.Delay(2000));
                }
                catch (Exception ex)
                {
                    logAction?.Invoke($"[CANCEL-EXCEPTION] {ex.GetType().Name}. Details: {ex}");
                }

                // 💥 Force kill MongoClient
                if (clientToKill != null)
                {
                    try
                    {
                        logAction?.Invoke($"[CLIENT-KILL] Disposing MongoClient due to timeout...");
                        (clientToKill as IDisposable)?.Dispose();
                        logAction?.Invoke($"[CLIENT-KILL-DONE] MongoClient disposed.");
                    }
                    catch (Exception ex)
                    {
                        logAction?.Invoke($"[CLIENT-KILL-ERROR] {ex.GetType().Name}. Details: {ex}");
                    }
                }

                logAction?.Invoke($"[TIMEOUT-END] {operationName} aborted after {(DateTime.UtcNow - startTime).TotalSeconds:F1}s.");
                throw new TimeoutException($"{operationName} timed out after {timeoutSeconds}s");
            }

            // -----------------------------
            // SUCCESS OR FAILURE
            // -----------------------------
            try
            {
                var result = await workerTask;
                logAction?.Invoke($"[SUCCESS] {operationName} finished in {(DateTime.UtcNow - startTime).TotalSeconds:F1}s.");
                return result;
            }
            catch (Exception ex)
            {
                logAction?.Invoke($"[FAILURE] {operationName} failed after {(DateTime.UtcNow - startTime).TotalSeconds:F1}s. Details: {ex}");

                // on failure, kill client too
                if (clientToKill != null)
                {
                    logAction?.Invoke($"[CLIENT-KILL] Disposing MongoClient due to failure...");
                    try { (clientToKill as IDisposable)?.Dispose(); }
                    catch { /* ignore */ }
                }

                throw;
            }
        }
    }


}
