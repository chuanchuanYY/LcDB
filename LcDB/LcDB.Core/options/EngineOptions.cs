using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core.options;

public enum IndexerType { 
    Dictionary,

}
public class EngineOptions
{
    public string DataDir { get; set; }
    public UInt64 MaxFileSize { get; set; }

    public IndexerType IndexerType { get; set; }

}
