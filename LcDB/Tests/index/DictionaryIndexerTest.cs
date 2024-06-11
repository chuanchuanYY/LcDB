using LcDB.Core.common;
using LcDB.Core.data;
using LcDB.Core.index;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tests.indexTest;

internal class DictionaryIndexerTest
{
    [SetUp]
    public void SetUp()
    {
        
    }

    [Test]
    public void Test_Put()
    {
        var index = new DictionaryIndexer();
        var record_pos1 = new LogRecordPos { FileID = 0,Offset=0 };
        var key1 = "key".ToBytes();
        var result1=index.Put(key1, record_pos1);
        Assert.IsTrue(result1);

        var record_pos2 = new LogRecordPos { FileID = 0, Offset = 0 };
        var result2 = index.Put("key2".ToBytes(), record_pos2);
        Assert.IsTrue(result2);

        // 插入相同 key的情况
        var record_pos3 = new LogRecordPos { FileID = 0, Offset = 0 };
        var result3 = index.Put(key1, record_pos3);
        Assert.IsFalse(result3);

        // 不使用同一个 key 的引用
        var record_pos4 = new LogRecordPos { FileID = 0, Offset = 0 };
        var result4 = index.Put("key".ToBytes(), record_pos3);
        Assert.IsFalse(result4);

        // 插入空 key 情况
        var record_pos5 = new LogRecordPos { FileID = 0, Offset = 0 };
        Assert.Catch(typeof(ArgumentNullException), () => { 
            index.Put("".ToBytes(), record_pos5);
        });


        // 测试插入100 万条数据情况
        var st = Stopwatch.StartNew();
        st.Start();
        for (int i = 1; i <= 100_0000; i++)
        {
            var record_pos6 = new LogRecordPos { FileID = (uint)i, Offset = (uint)i };
            var result6 = index.Put(Encoding.UTF8.GetBytes($"bitcask-key{i}"), record_pos6);
            if (!result6)
            {
                Console.WriteLine(i);
            }
            Assert.IsTrue(result6);
        }
        st.Stop();
        Console.WriteLine($"插入100_0000 条数据耗时：{st.ElapsedMilliseconds} ms");
    }

    [Test]
    public void Test_Get()
    {
        var index = new DictionaryIndexer();
        var record_pos1 = new LogRecordPos { FileID = 1, Offset = 0 };
        var key1 = "key1".ToBytes();
        var result1 = index.Put(key1, record_pos1);
        Assert.IsTrue(result1);

        var record_pos2 = new LogRecordPos { FileID = 0, Offset = 0 };
        var result2 = index.Put("key2".ToBytes(), record_pos2);
        Assert.IsTrue(result2);

        // 
        var value_result1 = index.Get(key1);
        Assert.IsTrue(value_result1 != null);
        Assert.IsTrue(value_result1.FileID == record_pos1.FileID);
        Assert.IsTrue(value_result1.Offset == record_pos1.Offset);


        var value_result2 = index.Get("key2".ToBytes());
        Assert.IsTrue(value_result2 != null);
       

        // 获取不存在key 的情况
        Assert.Catch(typeof(ArgumentNullException), () => {
            index.Get("".ToBytes());
        });

        var value_result4 = index.Get("None".ToBytes());
        Assert.IsTrue(value_result4 == null);

    }

    [Test]

    public void Test_Delete()
    {
        var index = new DictionaryIndexer();
        var record_pos1 = new LogRecordPos { FileID = 0, Offset = 0 };
        var result1 = index.Put("key".ToBytes(), record_pos1);
        Assert.IsTrue(result1);

        var record_pos2 = new LogRecordPos { FileID = 0, Offset = 0 };
        var result2 = index.Put("key2".ToBytes(), record_pos2);
        Assert.IsTrue(result2);


        var result3 = index.Delete("key".ToBytes());
        Assert.IsTrue(result3);

        var result4 = index.Delete("key2".ToBytes());
        Assert.IsTrue(result4);


    }
}
