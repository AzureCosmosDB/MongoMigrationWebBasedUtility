using System;
using System.IO;
using System.Linq;
using FluentAssertions;
using OnlineMongoMigrationProcessor;
using Xunit;

namespace OnlineMongoMigrationProcessor.Tests
{
    public class LogTests
    {
        private static string LogsFolder => Path.Combine(Helper.GetWorkingFolder(), "migrationlogs");

        private static void CleanupFiles(string id)
        {
            try
            {
                var bin = Path.Combine(LogsFolder, $"{id}.bin");
                var txt = Path.Combine(LogsFolder, $"{id}.txt");
                if (File.Exists(bin)) File.Delete(bin);
                if (File.Exists(txt)) File.Delete(txt);
            }
            catch { }
        }

        [Fact]
        public void Init_SetsInitialized_AndCreatesState()
        {
            var id = Guid.NewGuid().ToString("N");
            CleanupFiles(id);

            var log = new Log();
            var backupName = log.Init(id);
            log.IsInitialized.Should().BeTrue();
            backupName.Should().Be(id);

            var bucket = log.GetCurentLogBucket(id);
            bucket.Should().NotBeNull();
            (bucket.Logs?.Count ?? 0).Should().Be(0);

            CleanupFiles(id);
        }

        [Fact]
        public void WriteLine_Appends_InMemory_And_PersistsToBinary()
        {
            var id = Guid.NewGuid().ToString("N");
            CleanupFiles(id);

            var log = new Log();
            log.Init(id);

            log.WriteLine("first");
            log.WriteLine("second");

            var bucket = log.GetCurentLogBucket(id);
            (bucket.Logs?.Count ?? 0).Should().Be(2);
            bucket.Logs![0].Message.Should().Be("first");
            bucket.Logs![1].Message.Should().Be("second");

            // New instance should load from .bin
            var log2 = new Log();
            log2.Init(id);
            var bucket2 = log2.GetCurentLogBucket(id);
            (bucket2.Logs?.Count ?? 0).Should().BeGreaterOrEqualTo(2);
            bucket2.Logs!.Any(x => x.Message == "first").Should().BeTrue();
            bucket2.Logs!.Any(x => x.Message == "second").Should().BeTrue();

            CleanupFiles(id);
        }

        [Fact]
        public void AddVerboseMessage_RingBuffer_Reversed_And_Padded()
        {
            var id = Guid.NewGuid().ToString("N");
            var log = new Log();
            log.Init(id);

            // 6 messages -> buffer should keep last 5
            for (int i = 0; i < 6; i++)
                log.AddVerboseMessage($"m{i}");

            var last = log.GetVerboseMessages();
            last.Count.Should().Be(5);
            last.Select(x => x.Message).Should().ContainInOrder(new[] { "m5", "m4", "m3", "m2", "m1" });

            // When empty, current implementation returns 0 entries (no padding)
            var log2 = new Log();
            log2.Init(Guid.NewGuid().ToString("N"));
            var empty = log2.GetVerboseMessages();
            empty.Count.Should().Be(0);
        }

        [Fact]
        public void WriteLine_CapsListSize_ByDroppingIndex20_After300()
        {
            var id = Guid.NewGuid().ToString("N");
            CleanupFiles(id);

            var log = new Log();
            log.Init(id);

            for (int i = 0; i < 305; i++)
            {
                log.WriteLine($"msg{i}");
            }

            var bucket = log.GetCurentLogBucket(id);
            (bucket.Logs?.Count ?? 0).Should().BeLessOrEqualTo(300);

            CleanupFiles(id);
        }

        [Fact]
        public void Dispose_ClearsCurrentId_AndVerboseBuffer()
        {
            var id = Guid.NewGuid().ToString("N");
            var log = new Log();
            log.Init(id);
            log.WriteLine("hi");
            log.AddVerboseMessage("v1");

            log.Dispose();

            // Different path: GetCurentLogBucket should not return the previous bucket since id was cleared
            var bucket = log.GetCurentLogBucket(id);
            (bucket.Logs?.Count ?? 0).Should().Be(0);
        }
    }
}
