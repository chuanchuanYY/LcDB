using FriendlyCSharp.Databases;
using LcDB.Core.data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core.index;

/// <summary>
///  先使用第三方BTree 实现
/// </summary>
public class FcsBTreeIndexer 
{
    public bool Delete(byte[] key)
    {
        throw new NotImplementedException();
    }

    public LogRecordPos Get(byte[] key)
    {
        throw new NotImplementedException();
    }

    public List<byte[]> ListKeys()
    {
        throw new NotImplementedException();
    }

    public bool Put(byte[] key, LogRecordPos value)
    {
        throw new NotImplementedException();
    }
}

