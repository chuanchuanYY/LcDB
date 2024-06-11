using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core.index
{
    public class DictionaryIndexer : IndexerInterface
    {
        public DictionaryIndexer()
        {
            _index = new ConcurrentDictionary<byte[], byte[]>(new BytesCompare());
        }
        private ConcurrentDictionary<byte[], byte[]> _index;
        public bool Delete(Span<byte> key)
        {
            if (key.Length <= 0)
            {
                throw new ArgumentNullException(nameof(key));
            }
            return _index.Remove(key.ToArray(),out byte[]? value);
        }

        public Span<byte> Get(Span<byte> key)
        {
            if (key.Length <= 0)
            {
                throw new ArgumentNullException(nameof(key));
            }
            var ok =_index.
                TryGetValue(key.ToArray(),out byte[]? value);

            if (!ok) 
            {
                return Span<byte>.Empty;
            }

            return value;
        }

        public bool Put(Span<byte> key, Span<byte> value)
        {
            if (key.Length <= 0)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return _index.TryAdd(key.ToArray(),value.ToArray());
        }
    }

    internal class BytesCompare : EqualityComparer<byte[]?>
    {
        public override bool Equals(byte[]? x, byte[]? y)
        {
            if (x == null || y == null || x.Length != y.Length)
            {
                return false;
            }
            for (int i = 0; i < x.Length; i++)
            {
                if (x[i] != y[i])
                {
                    return false;
                }
            }
            return true;
        }
        public override int GetHashCode([DisallowNull] byte[]? obj)
        {
            if (obj == null || obj.Length<= 0)
                return 0;
            int reuslt = obj[0];
            for (int i = 1; i < obj.Length; i++)
            {
                reuslt ^= obj[i];
            }
            return reuslt;
        }
    }
}
