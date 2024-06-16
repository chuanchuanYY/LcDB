using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core.options;

public class WriteBatchOptions
{
    /// <summary>
    /// 设置一批次最大的写入数量
    /// </summary>
    public uint MaxWriteBatchNum { get; set; } = 100_0000;
}
