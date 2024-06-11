using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core.common
{
    public static class StringExtension
    {
        public static byte[] ToBytes(this string str)
        {
            return Encoding.UTF8.GetBytes(str);
        }
        public static string ToString(this byte[] bytes) 
        {
            return Encoding.UTF8.GetString(bytes);
        }
        public static string ToString(this Span<byte> bytes)
        {
            return Encoding.UTF8.GetString(bytes);
        }
    }
}
