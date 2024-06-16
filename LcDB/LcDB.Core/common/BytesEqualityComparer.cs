using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core.common;


public class BytesEqualityComparer : IEqualityComparer<byte[]>
{
    //public override bool Equals(byte[]? x, byte[]? y)
    //{
    //    //if (x == null || y == null || x.Length != y.Length)
    //    //{
    //    //    return false;
    //    //}
    //    //for (int i = 0; i < x.Length; i++)
    //    //{
    //    //    if (x[i] != y[i])
    //    //    {
    //    //        return false;
    //    //    }
    //    //}
    //    //return true;

    //    return StructuralComparisons.StructuralEqualityComparer.Equals(x,y);
    //}
    //public override int GetHashCode([DisallowNull] byte[]? obj)
    //{
    //    return StructuralComparisons.StructuralEqualityComparer.GetHashCode(obj);
    //}
    public bool Equals(byte[]? x, byte[]? y)
    {
        return StructuralComparisons.StructuralEqualityComparer.Equals(x, y);
    }

    public int GetHashCode([DisallowNull] byte[] obj)
    {
        return StructuralComparisons.StructuralEqualityComparer.GetHashCode(obj);
    }
}
