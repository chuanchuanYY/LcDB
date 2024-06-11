using LcDB.Core.data;
using LcDB.Core.index;
using LcDB.Core.io;
using LcDB.Core.options;
using System;
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
            default : _indexer = new DictionaryIndexer(); break;
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
                                 .Select(f=> Path.GetFileName(f).Split('.')[0])
                                 .Order()
                                 .Select(f =>
                                 {
                                     var option = new DataFileOptions()
                                     {
                                         DirPath = dir,
                                     };
                                     var file_id = Path.GetFileName(f).Split('.')[0];
                                     return new DataFile(option, uint.Parse(file_id));
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
            var record_bytes = dataFile.ReadLogRecord(offset);
            if (record_bytes.IsEmpty)
            {
                break;
            }

            // 解码 将字节数组 解析为 记录 LogRecord
            var record = new LogRecord(record_bytes);
            var record_pos = new LogRecordPos
            {
                FileID = dataFile.GetFileId(),
                Offset = offset,
            };
            _indexer.Put((byte[])record.Key.Clone(), record_pos.ToBytes());
            offset += (uint)record_bytes.Length;
        };
        GC.Collect();
    }
    public bool put(Span<byte> key, Span<byte> value) 
    {
        if (key == null || key.IsEmpty)
        {
            throw new ArgumentNullException(nameof(key));
        }

        var record = new LogRecord {
            Type = LogRecordType.NORMAL,
            KeySize = (uint)key.Length,
            ValueSize = (uint)value.Length,
            Key = key.ToArray(),
            Value = value.ToArray(),
        };

        //判断当前写入 是否 超过文件最大阈值
        var record_bytes = record.ToBytesSpan();
        if (_activeFile.GetOffset() + (UInt64)record_bytes.Length > _options.MaxFileSize)
        {
            var options = new DataFileOptions {
                DirPath = _options.DataDir,
            };
            var new_data_file = new DataFile(options, _activeFile.GetFileId() + 1);
            _oldDataFile.TryAdd(_activeFile.GetFileId(), _activeFile);
            _activeFile = new_data_file;
        }

        return _activeFile.WriteLogRecord(record_bytes);
    }
    public Span<byte> Get(Span<byte> key)
    {
        if (key == null || key.IsEmpty)
        {
            throw new InvalidDataException("key can not be null or empty");
        }
        // 先从内存索引查找数据
        var record_pos_bytes = _indexer.Get(key);
        if (record_pos_bytes.IsEmpty)
        {
            return Span<byte>.Empty;
        }

        var record_pos = new LogRecordPos(record_pos_bytes);
        // 如果有数据，再去磁盘查找
        if (record_pos.FileID == _activeFile.GetFileId())
        {
            return  _activeFile.ReadLogRecord(record_pos.Offset);
        }

        var ok=_oldDataFile.TryGetValue(record_pos.FileID,out var data_file);
        if (!ok)
        {
            throw new DataFileNotFoundExeption("datafile not found");
        }


        return data_file!.ReadLogRecord(record_pos.Offset);

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
