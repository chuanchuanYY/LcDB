using LcDB.Core.common;
using LcDB.Core.data;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core.index;
// 数据库插入，启动慢是索引器的问题，
// 索引器的问题是在比较器上

public class DictionaryIndexer : IndexerInterface
{
    public DictionaryIndexer()
    {
        _index = new ConcurrentDictionary<byte[],LogRecordPos>(new BytesEqualityComparer());
    }

    private ConcurrentDictionary<byte[], LogRecordPos> _index;

    public bool Delete(byte[] key)
    {
        if (key.Length <= 0)
        {
            throw new ArgumentNullException(nameof(key));
        }
        return _index.Remove(key,out LogRecordPos? value);
    }

    public LogRecordPos? Get(byte[] key)
    {
        if (key.Length <= 0)
        {
            throw new ArgumentNullException(nameof(key));
        }
        var ok =_index. TryGetValue(key,out LogRecordPos? value);

        if (!ok) 
        {
            return null;
        }
        return value;
    }

    public bool Put(byte[] key,LogRecordPos value)
    {
        if (key == null || key.Length <= 0)
        {
            throw new ArgumentNullException(nameof(key));
        }
        return _index.TryAdd(key,value);
    }

    public List<byte[]> ListKeys()
    {
        return _index.Keys.ToList();
    }

    public bool Update(byte[] key, LogRecordPos value)
    {
        var result = _index.AddOrUpdate(key,value,(old_key,old_value)=>value);
        if (result == null)
        {
            return false;
        }
        return true;
    }
}
