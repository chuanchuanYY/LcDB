using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tests.tools
{
    internal static class BytesExtension
    {
        public static void Dump(this byte[] bytes)
        {
            Console.WriteLine("");
            Console.Write("[");
            for (int i = 0; i < bytes.Length; i++)
            {
                Console.Write(bytes[i].ToString("X") + " ");
            }
            Console.Write("]");
        }
        public static void Dump(this Span<byte> bytes)
        {
            Console.WriteLine("");
            Console.Write("[");
            for (int i = 0; i < bytes.Length; i++)
            {
                Console.Write(bytes[i].ToString("X") + " ");
            }
            Console.Write("]");
        }
        public static bool BytesEqual(this byte[] left, byte[] right)
        {
            if(left.Length != right.Length )
                return false;
            for (int i = 0; i < left.Length; i++)
            {
                if (left[i] != right[i])
                {
                    return false;
                }
            }
            return true;
        }

    }
}
