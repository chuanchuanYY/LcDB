using LcDB.Core.data;
using LcDB.Core.index;
using LcDB.Core.io;
using LcDB.Core.options;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core.db;

public class Engine : IDisposable 
{
    private IndexerInterface _indexer;
    private DataFile _activeFile;
    private EngineOptions _options;
    private ConcurrentDictionary<uint,DataFile> _oldDataFile;

    public Engine(EngineOptions options)
    {
        _options = options;
        switch (options.IndexerType)
        {
            case IndexerType.Dictionary:
                _indexer = new DictionaryIndexer();
                break;
            default : _indexer = new DictionaryIndexer();
                break;
        }
        _oldDataFile = new ConcurrentDictionary<uint,DataFile>();
        Load_DataFile();

    }

    /// <summary>
    /// 在启动时 加载（初始化）数据文件
    /// </summary>
    private void Load_DataFile()
    {
        var dir = _options.DataDir;
        if (!Directory.Exists(dir))
        {
            Directory.CreateDirectory(dir);
        }

        // 获取所有的数据文件
        var datafiles = Directory.EnumerateFiles(dir)
                                 .Select(f => uint.Parse(Path.GetFileName(f).Split('.')[0]))
                                 .Order()
                                 .Select(f_id =>
                                 {
                                     var option = new DataFileOptions()
                                     {
                                         DirPath = dir,
                                     };
                                     return new DataFile(option, f_id);
                                 })
                                 .ToList();

        // 如果当前没有数据文件
        if (datafiles.Count <= 0)
        {
            // 创建第一个数据文件
            var option = new DataFileOptions()
            {
                DirPath = dir,
            };
            var new_datafile = new DataFile(option, 0);
            _activeFile = new_datafile;
        }
        else 
        {
            // 获取id最大的作为 当前活跃文件
             _activeFile = datafiles.TakeLast(1).First();
            datafiles.Remove(_activeFile);
        }
        

        // 将其余的数据文件文件，添加到旧的数据文件 集合
        datafiles.ForEach(f => {
            _oldDataFile.TryAdd(f.GetFileId(),f);
        });

        // 初始化 内存索引
        Load_Index_FromDataFile(_activeFile);
        datafiles.ForEach(f =>
        {
            Load_Index_FromDataFile(f);
        });
    }

    private void Load_Index_FromDataFile(DataFile dataFile)
    {
        uint offset = 0;
        while (true)
        {
            var record = dataFile.ReadLogRecord(ref offset);
            if (record == null)
            {
                break; // 读取完文件了
            }

            // 查看LogRecord的类型是否是Delete
            if (record.Type == LogRecordType.DELETED)
            {
                // 如果是delete 继续读取下一条数据，不添加到内存索引
                //删除内存索引，如果有
                _indexer.Delete(record.Key);
                continue;
            }
            
            var record_pos = new LogRecordPos
            {
                FileID = dataFile.GetFileId(),
                Offset = offset,
            };
            _indexer.Put(record.Key, record_pos);
        };
    }
    public bool put(byte[] key, byte[] value) 
    {
        if (key == null || key.Length <=0 )
        {
            throw new ArgumentNullException(nameof(key));
        }

        // 构建一条Log记录
        var record = new LogRecord {
            Type = LogRecordType.NORMAL,
            KeySize = (uint)key.Length,
            ValueSize = (uint)value.Length,
            Key = key,
            Value = value,
        };

        return AppendRecord(record);
    }
    private bool AppendRecord(LogRecord record)
    {
        //判断当前写入 是否 超过文件最大阈值
        var record_bytes = record.ToBytes();
        if (_activeFile.GetOffset() + record_bytes.Length > _options.MaxFileSize)
        {
            var options = new DataFileOptions
            {
                DirPath = _options.DataDir,
            };
            var new_data_file = new DataFile(options, _activeFile.GetFileId() + 1);
            _oldDataFile.TryAdd(_activeFile.GetFileId(), _activeFile);
            _activeFile = new_data_file;
        }

        var write_result = _activeFile.WriteLogRecord(record_bytes);
        if (write_result)
        {
            // 成功写入磁盘后 ，添加内存索引
            _indexer.Put(record.Key, new LogRecordPos
            {
                FileID = _activeFile.GetFileId(),
                Offset = _activeFile.GetOffset() - (uint)record_bytes.Length,
            });
        }
        return write_result;
    }
    public byte[]? Get(byte[] key)
    {
        if (key == null || key.Length <= 0)
        {
            throw new InvalidDataException("key can not be null or empty");
        }
        // 先从内存索引查找数据
        var record_pos = _indexer.Get(key);
        if (record_pos == null)
        {
            return null;
        }

        // 有数据，再去磁盘查找
        var offset = record_pos.Offset;
        if (record_pos.FileID == _activeFile.GetFileId())
        {
            return  _activeFile.ReadLogRecord(ref offset)?.Value;
        }

        
        var ok=_oldDataFile.TryGetValue(record_pos.FileID,out var data_file);
        if (!ok)
        {
            throw new DataFileNotFoundExeption("datafile not found");
        }
        return data_file!.ReadLogRecord(ref offset)?.Value;

    }

    /// <summary>
    /// 根据Key 删除一条记录
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    public bool Delete(byte[] key)
    {
        if (key == null || key.Length == 0)
        {
            throw new ArgumentNullException("key can not be null");
        }

        // 先在内存索引查看是否存在 记录
        if (_indexer.Get(key) == null)
        {
            return false;
        }

        // 先写入数据文件，表示记录为以删除
        // 构建一条Log记录
        var record = new LogRecord
        {
            Type = LogRecordType.DELETED,// 设为Delete 类型
            KeySize = (uint)key.Length,
            ValueSize = 0,
            Key = key,
            Value = new byte[0],
        };
        // 追加写入 数据文件
        var append_result = AppendRecord(record);
        if (!append_result)
        {
            return false;
        }

        // 删除内存索引记录
        return _indexer.Delete(key);
    }


    /// <summary>
    ///  将数据从缓冲区同步到物理存储
    /// </summary>
    public void Sync()
    {
       _activeFile.Sync();
    }

    /// <summary>
    /// 获取所有的key
    /// </summary>
    /// <returns></returns>
    public List<byte[]> ListKeys()
    {
        return _indexer.ListKeys();
    }

    /// <summary>
    /// 获取迭代器
    /// </summary>
    public Iterator Iter() { 
        
        var items = new List<(byte[]?, byte[]?)>();
        var keys = _indexer.ListKeys();
        foreach ( var key in keys)
        {
            var value =this.Get(key);
            items.Add((key,value));
        }

        return  new Iterator(items!);
    }

    /// <summary>
    /// 遍历所有数据，执行委托。结束遍历，如果委托返回false
    /// </summary>
    /// <param name="fn">参数1是key ，参数2是value</param>
    public void Fold(Func<byte[], byte[],bool> fn) 
    {
        var iter = this.Iter();
        foreach (var(key,value) in iter)
        {
            var result =fn?.Invoke(key,value);
            if (result == false)
            {
                return;
            }
        }
    }

    
    public void Dispose()
    {
        _activeFile.Dispose();
        _indexer = null;
        foreach (var key in _oldDataFile.Keys)
        {
            _oldDataFile[key].Dispose();
        }
        _options = null;
    }

     

}
