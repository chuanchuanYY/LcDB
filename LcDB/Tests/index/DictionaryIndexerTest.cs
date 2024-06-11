using LcDB.Core.common;
using LcDB.Core.index;
using System;
using System.Collections.Generic;
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
        var result1=index.Put("key".ToBytes(),"value".ToBytes());
        Assert.IsTrue(result1);


        var result2 = index.Put("key2".ToBytes(), "value2".ToBytes());
        Assert.IsTrue(result2);

        // 插入相同 key的情况
        var result3 = index.Put("key".ToBytes(), "value3".ToBytes());
        Assert.IsFalse(result3);

        // 插入空 key 情况
        Assert.Catch(typeof(ArgumentNullException), () => { 
            index.Put("".ToBytes(),"value".ToBytes());
        });
    }

    [Test]
    public void Test_Get()
    {
        var index = new DictionaryIndexer();
        var result1 = index.Put("key".ToBytes(), "value".ToBytes());
        Assert.IsTrue(result1);

        var result2 = index.Put("key2".ToBytes(), "value2".ToBytes());
        Assert.IsTrue(result2);

        var value_result1 = index.Get("key".ToBytes());
        Assert.IsTrue(!value_result1.IsEmpty);
        Assert.IsTrue(StringExtension.ToString(value_result1).Equals("value"));

        var value_result2 = index.Get("key2".ToBytes());
        Assert.IsTrue(!value_result2.IsEmpty);
        Assert.IsTrue(StringExtension.ToString(value_result2).Equals("value2"));

        // 获取不存在key 的情况

        
        Assert.Catch(typeof(ArgumentNullException), () => {
            index.Get("".ToBytes());
        });

        var value_result4 = index.Get("None".ToBytes());
        Assert.IsTrue(value_result4.IsEmpty);

    }

    [Test]

    public void Test_Delete()
    {
        var index = new DictionaryIndexer();
        var result1 = index.Put("key".ToBytes(), "value".ToBytes());
        Assert.IsTrue(result1);


        var result2 = index.Put("key2".ToBytes(), "value2".ToBytes());
        Assert.IsTrue(result2);


        var result3 = index.Delete("key".ToBytes());
        Assert.IsTrue(result3);

        var result4 = index.Delete("key2".ToBytes());
        Assert.IsTrue(result4);


    }
}
