using LcDB.Core.data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core.index
{
    internal interface IIndexIterator 
    {
        /// <summary>
        /// 重新返回迭代器的起始位置
        /// </summary>
        void Rewind();

        /// <summary>
        /// 根据传入的key 查找第一个大于等于 key 的值 ，然后从这个值开始遍历。
        /// 
        /// </summary>
        /// <param name="key"></param>
        void Seek(byte[] key);

        (byte[],LogRecordPos) Next();
    }
}
