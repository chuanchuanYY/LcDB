using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core.data;

public enum LogRecordType { 
    NORMAL =1, // 普通的记录
    DELETED =2, // 已删除的记录 
}

public class LogRecord
{
    public LogRecordType Type { get; set; }
    public UInt32 KeySize { get; set; }
    public UInt32 ValueSize { get; set;}
    public byte[] Key { get; set; }
    public byte[] Value { get; set; }
    public UInt32 Crc { get; set; }

    public LogRecord()
    {
        
    }
    /// <summary>
    /// 从字节数组 解码为 LogRrcord
    /// </summary>
    /// <param name="from"></param>
    ///  /// +-------------- Head ---------------+
    /// +-----------+-----------+-----------+-----------+-----------+-----------+
    /// |  type     | KeySize   | ValueSize | key       | size      | crc       |
    /// +-----------+-----------+-----------+-----------+-----------+-----------+
    public LogRecord(byte[] from)
    {
        if (from == null || from.Length <= 0 || from.Length < GetLogRecordSize())
        {
            throw new ArgumentException("from is null or empty or not enough length");
        }

        switch (from[0])
        {
            case 1: this.Type = LogRecordType.NORMAL;
                break;
            case 2:
                this.Type = LogRecordType.DELETED;
                break;
        };
        Span<byte> from_span = from;
        KeySize = BitConverter.ToUInt32(from_span.Slice(1,4));
        ValueSize = BitConverter.ToUInt32(from_span.Slice(5,4));
        Key = from_span.Slice(9,(int)KeySize).ToArray();
        Value = from_span.Slice(9+(int)KeySize, (int)ValueSize).ToArray();
        Crc = BitConverter.ToUInt32(from_span.Slice(9+ (int)KeySize+ (int)ValueSize,4));
    }
    public static int GetHeadSize()
    {
        return 1 + 4 + 4;
    }
    public byte[] ToBytes()
    {
        var size = GetLogRecordSize();
        var span = new byte[size];
        int offset = 0;
        AppendToSpan(span, ref offset, new byte[] { (byte)Type });
        AppendToSpan(span, ref offset, BitConverter.GetBytes(KeySize));
        AppendToSpan(span, ref offset, BitConverter.GetBytes(ValueSize));
        AppendToSpan(span, ref offset, Key);
        AppendToSpan(span, ref offset, Value);
        AppendToSpan(span, ref offset, BitConverter.GetBytes(Crc));

        return span;
    }
    private long GetLogRecordSize()
    {
        var size = GetHeadSize() + KeySize + ValueSize + 4;
        return size;
    }
    private void AppendToSpan(byte[] buffer, ref int offset, byte[] value)
    {
        int i = offset;
        int j = 0;
        for (; i < offset + value.Length; i++, j++)
        {
            buffer[i] = value[j];
        }
        offset = i;
    }
}

