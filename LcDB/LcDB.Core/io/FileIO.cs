using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core.io
{
    public class FileIO : IOManagerInterface, IDisposable
    {
        /// <summary>
        /// 文件路径，完整文件路径包括文件名及后缀
        /// </summary>
        /// <param name="path">filepath</param>
        /// <exception cref="ArgumentNullException"></exception>
        public FileIO(string path)
        {
            if (String.IsNullOrEmpty(path))
            {
                throw new ArgumentNullException("path");
            }
            var options = new FileStreamOptions();
            options.Access = FileAccess.ReadWrite;
            options.Mode = FileMode.OpenOrCreate;
            _stream = new FileStream(path, options);
        }
        private FileStream _stream;

       

        /// <summary>
        /// 从偏移量开始，读取 buf.Length 个字节 到buf
        /// </summary>
        /// <param name="buf"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        /// <exception cref="IOException"></exception>
        public int Get(byte[] buf, uint offset)
        {
            if ( buf == null || buf.Length == 0)
            {
                return 0;
            }
            // 设置偏移量
            _stream.Position = offset;
            try
            {
                return _stream.Read(buf,0,buf.Length);
            }
            catch (Exception e)
            {
                throw new IOException($"failed to read data from fileStream:{e.Message}");
            }

        }

        public int Put(byte[] buf, uint offset)
        {
            if (buf == null || buf.Length == 0)
            {
                return 0;
            }
            // 设置偏移量
            _stream.Position = offset; 
            try
            {
                _stream.Write(buf, 0, buf.Length);
                _stream.Flush();
            }
            catch (Exception e)
            {
                throw new IOException($"failed to write data to fileStream,{e.Message}");
            }
            return buf.Length;
        }

        public void Dispose()
        {
            _stream.Dispose();
        }

        public void Close()
        {
            Dispose();
        }
    }
}
