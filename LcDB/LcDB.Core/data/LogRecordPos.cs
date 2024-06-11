using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core.data
{
    public class LogRecordPos
    {
        /// <summary>
        /// 文件 id ，表示该记录位于哪个文件
        /// </summary>
        public  uint FileID { get; set; }
        /// <summary>
        /// LogRecord 的偏移量 ，表示该在文件中的起始位置
        /// </summary>
        public uint Offset { get; set; }


        public LogRecordPos()
        {
            
        }

        public LogRecordPos(Span<byte> from)
        {
            FileID = BitConverter.ToUInt32(from.Slice(0,4));
            Offset = BitConverter.ToUInt32(from.Slice(4,4));
        }
        public byte[] ToBytes() {

            var result = new byte[4 + 4];
            var offset = 0;
            AppendToSpan(result, ref offset, BitConverter.GetBytes(FileID));
            AppendToSpan(result,ref offset,BitConverter.GetBytes(Offset));
            return result;
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
}
