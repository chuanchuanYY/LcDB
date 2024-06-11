using LcDB.Core.io;
using LcDB.Core.options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core.data
{
    public class DataFile:IDisposable
    {
         
        private IOManagerInterface _ioManager;
        public DataFile(DataFileOptions options ,uint file_id)
        {
            var path =Path.Combine(options.DirPath, file_id.ToString() + ".data");
            _ioManager = new FileIO(path);
            _fileId = file_id;
            _offset = 0;
        }
        private uint _fileId;
        /// <summary>
        /// 文件操作的偏移量 ，表示从 偏移量 _offset 开始写数据
        /// </summary>
        private uint _offset;
        public uint GetOffset() => _offset;
        public uint GetFileId() => _fileId;
        

        /// <summary>
        /// 写一条 LocRecord 记录
        /// </summary>
        /// <param name="logRecord"></param>
        public bool WriteLogRecord(byte[] log_record)
        {
            _offset += (uint)_ioManager.Put(log_record, _offset);
            return true;
        }

        /// <summary>
        /// 从偏移量 offset 开始读取一条 记录（LogRecord）
        /// 并将 offset 向后移动读取的长度
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        /// +-------------- Head ---------------+
        /// +-----------+-----------+-----------+-----------+-----------+
        /// |  type     | KeySize   | ValueSize | key       | size      |
        /// +-----------+-----------+-----------+-----------+-----------+
        public LogRecord? ReadLogRecord(ref uint offset)
        {
            //先读取固定长度head 的数据
            var record_head =new byte[LogRecord.GetHeadSize()];
            if (_ioManager.Get(record_head, offset) <= 0 )
            {
                return null;
            }
            // 查看记录的类型是否是已删除
            if (ByteToLogRecordType(record_head[0]) == LogRecordType.DELETED)
            {
                return null;
            }

            // 获取 keysize 和 value size 
            Span<byte> record_head_span = record_head;
            var key_size = BitConverter.ToUInt32(record_head_span.Slice(1, 4));
            var value_size = BitConverter.ToUInt32(record_head_span.Slice(5,4));

            var record_buf =  new byte[LogRecord.GetHeadSize() 
                + key_size + value_size + 4];
              var read_count = _ioManager.Get(record_buf, offset);
            if (read_count <= 0)
            {
                // 如果读取到的字节数为零 ，表示没有数据，或者读到文件末尾了
                return null;
            }

            // 解码 将字节数组 解析为 记录 LogRecord
            offset += (uint)record_buf.Length;
            return new LogRecord(record_buf);
        }

       

        private LogRecordType ByteToLogRecordType(byte b)
          => b switch
          {
              1 => LogRecordType.NORMAL,
              2 => LogRecordType.DELETED,
              _ => throw new ArgumentException("invalid data")
          };

        public void Dispose()
        {
           _ioManager.Close();
        }
    }
}
