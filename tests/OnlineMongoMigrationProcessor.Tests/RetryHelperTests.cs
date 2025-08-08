using System;
using System.Threading.Tasks;
using FluentAssertions;
using OnlineMongoMigrationProcessor;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using Xunit;

namespace OnlineMongoMigrationProcessor.Tests
{
    public class RetryHelperTests
    {
        private class TestLog : Log
        {
            public TestLog()
            {
                Init(Guid.NewGuid().ToString());
            }
        }

        [Fact]
        public async Task ExecuteTask_ReturnsSuccess_WhenTaskSucceedsImmediately()
        {
            var helper = new RetryHelper();
            var log = new TestLog();

            var result = await helper.ExecuteTask(
                () => Task.FromResult(TaskResult.Success),
                (ex, attempt, backoff) => Task.FromResult(TaskResult.Retry),
                log,
                maxTries: 3,
                initialDelayMs: 1);

            result.Should().Be(TaskResult.Success);
        }

        [Fact]
        public async Task ExecuteTask_RetriesOnRetryResult_AndEventuallySucceeds()
        {
            var helper = new RetryHelper();
            var log = new TestLog();
            int calls = 0;

            var result = await helper.ExecuteTask(
                () => Task.FromResult(++calls < 3 ? TaskResult.Retry : TaskResult.Success),
                (ex, attempt, backoff) => Task.FromResult(TaskResult.Retry),
                log,
                maxTries: 5,
                initialDelayMs: 1);

            result.Should().Be(TaskResult.Success);
            calls.Should().Be(3);
        }

        [Fact]
        public async Task ExecuteTask_AbortsOnExceptionHandlerAbort()
        {
            var helper = new RetryHelper();
            var log = new TestLog();
            int calls = 0;

            var result = await helper.ExecuteTask(
                () => Task.FromResult(++calls == 1 ? throw new InvalidOperationException("boom") : TaskResult.Success),
                (ex, attempt, backoff) => Task.FromResult(TaskResult.Abort),
                log,
                maxTries: 5,
                initialDelayMs: 1);

            result.Should().Be(TaskResult.Abort);
        }

        [Fact]
        public async Task ExecuteTask_FailsAfterMaxRetries()
        {
            var helper = new RetryHelper();
            var log = new TestLog();

            var result = await helper.ExecuteTask(
                () => Task.FromResult(TaskResult.Retry),
                (ex, attempt, backoff) => Task.FromResult(TaskResult.Retry),
                log,
                maxTries: 2,
                initialDelayMs: 1);

            result.Should().Be(TaskResult.FailedAfterRetries);
        }
    }
}
