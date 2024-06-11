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
    public uint MaxFileSize { get; set; }

    public IndexerType IndexerType { get; set; }

}
