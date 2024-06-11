using LcDB.Core.data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tests.tools;

namespace Tests.data;

internal class LogRecordPosTest
{
    [SetUp]
    public void SetUp()
    {
        
    }

    [Test]
    public void Test_LogRecord_Encode_Decode()
    {
        var record_pos = new LogRecordPos { 
             FileID = 1,
             Offset = 0,
        };
        var encode1 = record_pos.ToBytes();
        Assert.NotNull(encode1);
        Assert.IsTrue(encode1.Length != 0);
        encode1.Dump();


        // 解码 测试

        var decode_record_ops = new LogRecordPos(encode1);
        Assert.NotNull(decode_record_ops);
        Assert.IsTrue(decode_record_ops.ToBytes().BytesEqual(encode1));
        Assert.IsTrue(decode_record_ops.FileID == record_pos.FileID);
        Assert.IsTrue(decode_record_ops.Offset == record_pos.Offset);




        var record_pos2 = new LogRecordPos
        {
            FileID = 100,
            Offset = 1024,
        };
        var encode2 = record_pos2.ToBytes();
        Assert.NotNull(encode2);
        Assert.IsTrue(encode2.Length != 0);
        encode2.Dump();


        // 解码 测试

        var decode_record_ops2 = new LogRecordPos(encode2);
        Assert.NotNull(decode_record_ops2);
        Assert.IsTrue(decode_record_ops2.ToBytes().BytesEqual(encode2));
        Assert.IsTrue(decode_record_ops2.FileID == record_pos2.FileID);
        Assert.IsTrue(decode_record_ops2.Offset == record_pos2.Offset);

    }

}
