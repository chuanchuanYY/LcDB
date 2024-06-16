using LcDB.Core.data;
using LcDB.Core.db;
using LcDB.Core.options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core;

/// <summary>
/// 用于批量写入数据
/// </summary>
public class WriteBatch
{
    /// <summary>
    /// 将要写入的数据 暂存
    /// </summary>
    private ConcurrentDictionary<byte[],LogRecord> _records;

    private Engine _engine;

    private WriteBatchOptions _options;
    public WriteBatch(Engine engine,WriteBatchOptions options)
    {
        _records = new ConcurrentDictionary<byte[],LogRecord>();
        _engine = engine;
        _options = options;
    }
    public bool Put(byte[] key, byte[] value) {

        ArgumentNullException.ThrowIfNull(key, "key can not be null");
        var logrecord = new LogRecord {
            Type = LogRecordType.NORMAL,
            Key = key,
            Value = value,
            KeySize = (uint)key.Length,
            ValueSize = (uint)value.Length,
        };
        return  _records.TryAdd(key, logrecord);
    }

    public bool Delete(byte[] key) {

        ArgumentNullException.ThrowIfNull(key, "key can not be null");

        return _records.Remove(key,out var value);
    }


    /// <summary>
    /// 在执行提交的时候 ，所有的数据才会从占存区 写入到磁盘
    /// </summary>
    public bool Commit() {

        if (_records.IsEmpty)
        {
            return true;
        }

        var version = _engine.GetTransactionNo();
        uint write_count = 0;
        foreach (var key in _records.Keys.Reverse()) {
            
            if (write_count >= _options.MaxWriteBatchNum)
            {
                return true;
            }

            var record = _records[key];
            record.Version = version;
            var append_result = _engine.AppendRecord(record);
            if (append_result == false)
            {
                return false;
            }

            _records.Remove(key,out var _);
            write_count++;
        }

        // 写入最后一条记录，表示该批量写是完整的，有效的。

        var complete_record = new LogRecord {
            Type = LogRecordType.TRANSACTION,
            Key = new byte[0],
            Value = new byte[0],
            KeySize = 0,
            ValueSize = 0,
            Version = version,
        };
        var result = _engine.AppendRecord(complete_record);

        _engine.Sync();

        return result;
    }
}
