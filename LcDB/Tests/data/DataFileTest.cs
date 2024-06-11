using LcDB.Core.common;
using LcDB.Core.data;
using LcDB.Core.io;
using LcDB.Core.options;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tests.data
{
    internal class DataFileTest
    {
        [SetUp]
        public void SetUp()
        {
            
        }
        [Test]
        public void Test_Write_Record()
        {
            var path = Path.Combine(Path.GetTempPath(),"/DataFileWriteTest");
            var options = new DataFileOptions { 
                DirPath = path,
            };
            DataFile dataFile = new DataFile(options, 0);

            var log_record1 = new LogRecord() {
                Type = LogRecordType.NORMAL,
                Key = "abc".ToBytes(),
                Value = "bitcast-kv".ToBytes(),
                KeySize = 3,
                ValueSize = (uint)"bitcast-kv".ToBytes().Length,
            };
           var result1 =  dataFile.WriteLogRecord(log_record1.ToBytesSpan());
           Assert.IsTrue(result1);

            var log_record2 = new LogRecord()
            {
                Type = LogRecordType.NORMAL,
                Key = "bbb".ToBytes(),
                Value = "bitcast-kv2".ToBytes(),
                KeySize = 3,
                ValueSize = (uint)"bitcast-kv2".ToBytes().Length,
            };
            var result2 = dataFile.WriteLogRecord(log_record2.ToBytesSpan());
            Assert.IsTrue(result2);


            for (int i = 0; i < 300_0000;i++)
            {
                var log_record3 = new LogRecord()
                {
                    Type = LogRecordType.NORMAL,
                    Key = $"bbb{i}".ToBytes(),
                    Value = $"bitcast-kv{i}".ToBytes(),
                    KeySize = (uint)$"bbb{i}".ToBytes().Length,
                    ValueSize = (uint)$"bitcast-kv{i}".ToBytes().Length,
                };
                var result3 = dataFile.WriteLogRecord(log_record3.ToBytesSpan());
                Assert.IsTrue(result3);
            }
        }

        [Test]
        public void Test_Read_Record()
        {
            var path = Path.Combine(Path.GetTempPath(), "DataFileReadTest");
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            var options = new DataFileOptions
            {
                DirPath = path,
            };
            DataFile dataFile = new DataFile(options, 1);

            var log_record1 = new LogRecord()
            {
                Type = LogRecordType.NORMAL,
                Key = "abc".ToBytes(),
                Value = "bitcast-kv".ToBytes(),
                KeySize = 3,
                ValueSize = (uint)"bitcast-kv".ToBytes().Length,
            };
            var result1 = dataFile.WriteLogRecord(log_record1.ToBytesSpan());
            Assert.IsTrue(result1);

            var log_record2 = new LogRecord()
            {
                Type = LogRecordType.NORMAL,
                Key = "bbb".ToBytes(),
                Value = "bitcast-kv2".ToBytes(),
                KeySize = 3,
                ValueSize = (uint)"bitcast-kv2".ToBytes().Length,
            };
            var result2 = dataFile.WriteLogRecord(log_record2.ToBytesSpan());
            Assert.IsTrue(result2);

            for (int i = 0; i < 300_0000; i++)
            {
                var log_record3 = new LogRecord()
                {
                    Type = LogRecordType.NORMAL,
                    Key = $"bbb{i}".ToBytes(),
                    Value = $"bitcast-kv{i}".ToBytes(),
                    KeySize = (uint)$"bbb{i}".ToBytes().Length,
                    ValueSize = (uint)$"bitcast-kv{i}".ToBytes().Length,
                };
                var result3 = dataFile.WriteLogRecord(log_record3.ToBytesSpan());
                Assert.IsTrue(result3);
            }

            var result4 = dataFile.ReadLogRecord(0);
            Assert.IsTrue(!result4.IsEmpty);
            Assert.IsTrue(result4.Length == 26);

            var result5 = dataFile.ReadLogRecord(26);
            Assert.IsTrue(!result5.IsEmpty);
            Assert.IsTrue(result5.Length == 27);

            uint offset = 0;
            int record_count = 0;
            List<LogRecord> logRecords = new List<LogRecord>();
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            while (true)
            {
                var result6 = dataFile.ReadLogRecord(offset);
                if (result6.IsEmpty)
                {
                    break;
                }
                var lor_record = new LogRecord(result6);
                Assert.NotNull(lor_record);
                logRecords.Add(lor_record);
                record_count++;
                offset += (uint)result6.Length;
            }
            Assert.IsTrue(record_count -2  == 300_0000 );
            stopwatch.Stop();
            Console.WriteLine($"spend {stopwatch.ElapsedMilliseconds} ms");
            Assert.IsTrue(logRecords.Count -2 == 300_0000 );
            foreach (var item in logRecords.TakeLast(100))
            {
                Console.WriteLine($"key = {StringExtension.ToString(item.Key)}  Value = {StringExtension.ToString(item.Value)}");
            };
            dataFile.Dispose();
            Directory.GetFiles(path)
                .ToList()
                .ForEach(f => { 
                  File.Delete(f);
                });
            Directory.Delete(path);
        }

    }
}
