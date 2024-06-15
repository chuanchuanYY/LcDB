using LcDB.Core.data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core.index;

public interface IndexerInterface
{
    /// <summary>
    /// 插入一条索引记录
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public bool Put(byte[] key ,LogRecordPos value);

    /// <summary>
    /// 获取LogRecord 位置索引
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    public LogRecordPos? Get(byte[] key);

    /// <summary>
    /// 根据Key 删除一条索引
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    public bool Delete(byte[] key);

    /// <summary>
    /// 获取所有的key
    /// </summary>
    /// <returns></returns>
    public List<byte[]> ListKeys();
}

