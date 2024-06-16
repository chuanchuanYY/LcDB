using LcDB.Core.common;
using LcDB.Core.db;
using LcDB.Core.options;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tests.db
{
    internal class EngineTest
    {
        [SetUp]
        public void SetUp() {
        }


        [Test]
        public void Test_Put()
        {
            var path = Path.Combine(Path.GetTempPath(), "kvdbTest_C#_db_put");
            var option = new EngineOptions {
                DataDir = path,
                IndexerType = IndexerType.Dictionary,
                MaxFileSize = 1 * 1024 * 1024, 
            };
            var engine = new Engine(option);

           var result1= engine.put("key1".ToBytes(),"value1".ToBytes());
           Assert.IsTrue(result1);

            // 插入重复的 key
            var result2 = engine.put("key1".ToBytes(), "value1R".ToBytes());
            Assert.IsTrue(result2);

            // key 为空情况
            Assert.Catch(typeof(ArgumentNullException), () => {
                var result3 = engine.put("".ToBytes(), "value3".ToBytes());
            });

            // value 为空情况
            var result4 = engine.put("key4".ToBytes(), "".ToBytes());
            Assert.IsTrue(result4);


            // 写到超出单个文件的大小阈值
            var st =Stopwatch.StartNew();
            st.Start();
            for (int i = 0; i < 1_0000; i++)
            {
                var result5 = engine.put($"key{i}".ToBytes(), $"value{i}".ToBytes());
                Assert.IsTrue(result5);
            }
            st.Stop();
            Console.WriteLine($"写入1_0000 条数据耗时 {st.ElapsedMilliseconds} ms");
            // 重启数据库引擎 在put 情况
            // 先关闭之前的 
            engine.Dispose();

            
            var st2 = Stopwatch.StartNew();
            st2.Start();
            var engine2 = new Engine(option);
            st2.Stop();
            Console.WriteLine($"重启引擎耗时 {st2.ElapsedMilliseconds} ms");
            Assert.NotNull(engine2);
            for (int i = 0; i < 1_0000; i++)
            {
                var result6 = engine2.put($"key{i}".ToBytes(), $"value{i}".ToBytes());
                Assert.IsTrue(result6);
            }

            engine2.Dispose();

            Directory.GetFiles(path)
                .ToList()
                .ForEach(f =>
                {
                    File.Delete(f);
                });
            Directory.Delete(path);
        }


        [Test]
        public void Test_Get()
        {
            var path = Path.Combine(Path.GetTempPath(), "kvdbTest_C#_db_get");
            var option = new EngineOptions
            {
                DataDir = path,
                IndexerType = IndexerType.Dictionary,
                MaxFileSize = 1 * 1024 * 1024,
                
            };
            var engine = new Engine(option);

            var result1 = engine.put("key1".ToBytes(), "value1".ToBytes());
            Assert.IsTrue(result1);

            // 插入重复的 key
            var result2 = engine.put("key1".ToBytes(), "value1R".ToBytes());
            Assert.IsTrue(result2);

            // key 为空情况
            Assert.Catch(typeof(ArgumentNullException), () => {
                var result3 = engine.put("".ToBytes(), "value3".ToBytes());
            });

            // value 为空情况
            var result4 = engine.put("key4".ToBytes(), "".ToBytes());
            Assert.IsTrue(result4);


            var value1 = StringExtension.ToString(engine.Get("key1".ToBytes()));
            Console.WriteLine(value1);
            //  Assert.IsTrue(value1.Equals("value1"));
           
            
            engine.Dispose();

            Directory.GetFiles(path)
                .ToList()
                .ForEach(f =>
                {
                    File.Delete(f);
                });
            Directory.Delete(path);
        }


        [Test]
        public void Test_Delete()
        {
            var path = Path.Combine(Path.GetTempPath(), "kvdbTest_C#_db_delete");
            var option = new EngineOptions
            {
                DataDir = path,
                IndexerType = IndexerType.Dictionary,
                MaxFileSize = 1 * 1024 * 1024,
            };
            var engine = new Engine(option);

            // 删除空key
            Assert.Catch(typeof(ArgumentNullException), () => {
                engine.Delete("".ToBytes());
            });
            // 删除不存在的值情况
            var delete_result1 = engine.Delete("none".ToBytes());
            Assert.IsFalse(delete_result1);

            var put_result1 = engine.put("key1".ToBytes(), "value1".ToBytes());
            Assert.IsTrue(put_result1);

            var delete_result2 = engine.Delete("key1".ToBytes());
            Assert.IsTrue(delete_result2);

            // 删除后获取数据
            var get_result = engine.Get("key1".ToBytes());
            Assert.IsNull(get_result);

            var put_result2 = engine.put("key2".ToBytes(), "value2".ToBytes());
            Assert.IsTrue(put_result2);
            var put_result3 = engine.put("key3".ToBytes(), "value3".ToBytes());
            Assert.IsTrue(put_result3);
            var put_result4 = engine.put("key4".ToBytes(), "value4".ToBytes());
            Assert.IsTrue(put_result4);


            // 删除数据后重启引擎
            var delete_result3 =engine.Delete("key3".ToBytes());
            Assert.IsTrue(delete_result3);
            engine.Dispose();

            var engine2 = new Engine(option);
            var get_resutl2 =engine2.Get("key3".ToBytes());
            Assert.IsNull(get_resutl2);

            engine2.Dispose();
            Directory.GetFiles(path)
                .ToList()
                .ForEach(f =>
                {
                    File.Delete(f);
                });
            Directory.Delete(path);
        }
    }
}
