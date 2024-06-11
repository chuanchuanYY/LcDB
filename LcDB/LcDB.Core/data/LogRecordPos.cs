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
        public UInt64 Offset { get; set; }




        public LogRecordPos()
        {
            
        }

        public LogRecordPos(Span<byte> from)
        {
            //if (from.Length < 4 + 8)
            //{
            //    throw new ArgumentException("length too small");
            //}
            FileID = BitConverter.ToUInt32(from.Slice(0,4));
            Offset = BitConverter.ToUInt64(from.Slice(4, 8));
        }
        public byte[] ToBytes() {

            var result = new Span<byte>(new byte[sizeof(uint)+sizeof(UInt64)]);
            var offset = 0;
            AppendToSpan(result, ref offset, BitConverter.GetBytes(FileID));
            AppendToSpan(result,ref offset,BitConverter.GetBytes(Offset));
            return result.ToArray();
        }

        private void AppendToSpan(Span<byte> span, ref int offset, byte[] value)
        {
            int i = offset;
            int j = 0;
            for (; i < offset + value.Length; i++, j++)
            {
                span[i] = value[j];
            }
            offset = i;
        }
    }
}
