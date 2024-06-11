using LcDB.Core.common;
using LcDB.Core.io;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tests.io
{
    internal class FileIOTest
    {
        [SetUp]
        public void SetUp() { 
        
        }

        [Test]
        public void Test_Put()
        {
            var path = Path.Combine(Path.GetTempPath(),"fio0.data");
            var fio = new FileIO(path);
            
            var result1= fio.Put("Hello".ToBytes(),0);
            Assert.IsTrue(result1 == 5);

            var result2 = fio.Put("world".ToBytes(), 5);
            Assert.IsTrue(result2 == 5);

            var result3 = fio.Put("".ToBytes(), 0);
            Assert.IsTrue(result3 == 0);

        }

        [Test]
        public void Test_Get()
        {
            var path = Path.Combine(Path.GetTempPath(), "fio1.data");
            var fio = new FileIO(path);

            var result1 = fio.Put("Hello".ToBytes(), 0);
            Assert.IsTrue(result1 == 5);

            var result2 = fio.Put("world".ToBytes(), 5);
            Assert.IsTrue(result2 == 5);

            var result3 = fio.Put("".ToBytes(), 0);
            Assert.IsTrue(result3 == 0);

            var buf1 = new Span<byte>(new byte[5]);
            var read_count = fio.Get(buf1,0);
            Assert.IsTrue(read_count == 5);
            Assert.IsTrue(StringExtension.ToString(buf1).Equals("Hello"));

            var buf2 = new Span<byte>(new byte[5]);
            var read_count2 = fio.Get(buf2, 5);
            Assert.IsTrue(read_count2 == 5);
            Assert.IsTrue(StringExtension.ToString(buf2).Equals("world"));

            var buf3 = new Span<byte>(new byte[0]);
            var read_count3 = fio.Get(buf3, 0);
            Assert.IsTrue(read_count3 == 0);

        }
    }
}
