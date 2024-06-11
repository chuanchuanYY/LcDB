using LcDB.Core.common;
using LcDB.Core.data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tests.tools;

namespace Tests.data
{
    internal class LogRecordTest
    {
        [SetUp]
        public void SetUp()
        {

        }

        [Test]
        public void Test_LogRecord_Encode_Decode()
        {
            var key1 = "abc".ToBytes();
            var value1 = "hello".ToBytes();
            var record1 = new LogRecord { 
                Type = LogRecordType.NORMAL,
                KeySize = (uint)key1.Length,
                ValueSize = (uint)value1.Length,
                Key = key1,
                Value = value1,
            };
            var encode1 = record1.ToBytesSpan();
            encode1.Dump();

            // 测试解码

            var decode_record = new LogRecord(encode1);

            Assert.IsTrue(decode_record != null);
            Assert.IsTrue(decode_record.Type == record1.Type);
            Assert.IsTrue(decode_record.KeySize == record1.KeySize);
            Assert.IsTrue(decode_record.ValueSize == record1.ValueSize);

            for (int i = 0; i < decode_record.KeySize; i++)
            {
                Assert.IsTrue(decode_record.Key[i] == record1.Key[i]);
            }
            for (int i = 0; i < decode_record.ValueSize; i++)
            {
                Assert.IsTrue(decode_record.Value[i] == record1.Value[i]);
            }
            Assert.IsTrue(decode_record.Crc == record1.Crc);



            var key2 = "abc".ToBytes();
            var value2 = "hello world".ToBytes();
            var record2 = new LogRecord
            {
                Type = LogRecordType.DELETED, // delete type 
                KeySize = (uint)key2.Length,
                ValueSize = (uint)value2.Length,
                Key = key2,
                Value = value2,
            };
            var encode2 = record2.ToBytesSpan();
            encode2.Dump();

            // 测试解码

            var decode_record2 = new LogRecord(encode2);

            Assert.IsTrue(decode_record2 != null);
            Assert.IsTrue(decode_record2.Type == record2.Type);
            Assert.IsTrue(decode_record2.KeySize == record2.KeySize);
            Assert.IsTrue(decode_record2.ValueSize == record2.ValueSize);

            for (int i = 0; i < decode_record2.KeySize; i++)
            {
                Assert.IsTrue(decode_record2.Key[i] == record2.Key[i]);
            }
            for (int i = 0; i < decode_record2.ValueSize; i++)
            {
                Assert.IsTrue(decode_record2.Value[i] == record2.Value[i]);
            }
            Assert.IsTrue(decode_record2.Crc == record2.Crc);
        }

       

    }
}
