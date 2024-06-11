using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core;

public class DataFileNotFoundExeption : Exception
{
    public DataFileNotFoundExeption(string message):base(message)
    {
        
    }
}