using LcDB.Core.common;
using LcDB.Core.db;
using LcDB.Core.options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tests;

internal class WriteBatchTest
{

    [SetUp]
    public void SetUp()
    { 
    }

    [Test]
    public void Test_WriteBatch1()
    {
        var path = Path.Combine(Path.GetTempPath(), "kvdbTest_C#_write_batch1");
        var option = new EngineOptions
        {
            DataDir = path,
            IndexerType = IndexerType.Dictionary,
            MaxFileSize = 1 * 1024 * 1024,

        };
        var engine = new Engine(option);


        var wb = engine.WriteBatch(new WriteBatchOptions());
        var put_result1 = wb.Put("writebatch1".ToBytes(),"value1".ToBytes());
        Assert.IsTrue(put_result1);
        var commit_result1 = wb.Commit();
        Assert.IsTrue(commit_result1);

        var put_result2 = wb.Put("writebatch2".ToBytes(), "value2".ToBytes());
        Assert.IsTrue(put_result2);
        var put_result3 = wb.Put("writebatch3".ToBytes(), "value3".ToBytes());
        Assert.IsTrue(put_result3);
        var commit_result2 = wb.Commit();
        Assert.IsTrue(commit_result2);

        var get_result1 =  engine.Get("writebatch2".ToBytes());
        Assert.NotNull(get_result1);
        //Console.WriteLine(Encoding.UTF8.GetString(get_result1));

        var get_result2 = engine.Get("writebatch1".ToBytes());
        Assert.NotNull(get_result2);

        engine.Dispose();
        // 重启后再批量写
        var engine2 = new Engine(option);
        
        var wb2 = engine2.WriteBatch(new WriteBatchOptions { 
             MaxWriteBatchNum = 1_000,
        });
        var put_result4 = wb2.Put("writebatch4".ToBytes(), "value4".ToBytes());
        Assert.IsTrue(put_result4);
        var commit_result3 =wb2.Commit();
        Assert.IsTrue(commit_result3);




        engine2.Dispose();


        Directory.GetFiles(path)
            .ToList()
            .ForEach(f =>
            {
                File.Delete(f);
            });
        Directory.Delete(path);

    }
}
