using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core.index;

public interface IndexerInterface
{
    public bool Put(Span<byte> key , Span<byte> value);

    public Span<byte> Get(Span<byte> key);

    public bool Delete(Span<byte> key);
}

