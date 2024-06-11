using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core.io;

public interface IOManagerInterface
{
    /// <summary>
    /// 根据偏移量写入 数据
    /// </summary>
    /// <param name="buf">要写入数据</param>
    /// <param name="offset">偏移量</param>
    /// <returns>写入的字节数量 </returns>
    public ulong Put(Span<byte> buf, ulong offset);

    /// <summary>
    /// 从偏移量开始读取数据到 buf  
    /// </summary>
    /// <param name="buf">接收数据缓冲区</param>
    /// <param name="offset">偏移量</param>
    /// <returns>读取的字节数</returns>
    public int Get(Span<byte> buf, ulong offset);


    /// <summary>
    /// 提供方法用于关闭IO接口
    /// </summary>
    public void Close();

}
