using FriendlyCSharp.Databases;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core.index;

/// <summary>
///  先使用第三方BTree 实现
/// </summary>
public class FcsBTreeIndexer : IndexerInterface
{

   
    public bool Delete(Span<byte> key)
    {
        throw new NotImplementedException();
    }

    public Span<byte> Get(Span<byte> key)
    {
        throw new NotImplementedException();
    }

    public bool Put(Span<byte> key, Span<byte> value)
    {
        throw new NotImplementedException();
    }
}

