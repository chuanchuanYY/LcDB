using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core.io
{
    public class FileIO : IOManagerInterface, IDisposable
    {
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

       

        public int Get(Span<byte> buf, ulong offset)
        {
            if ( buf == null || buf.IsEmpty)
            {
                return 0;
            }
            _stream.Position = (long)offset;

            try
            {
                var buffer = buf.ToArray();
                var result = _stream.Read(buffer, 0,buf.Length);
                buffer.CopyTo(buf);
                return result;
            }
            catch (Exception e)
            {

                throw new IOException($"failed to read data from fileStream:{e.Message}");
            }

        }

        public ulong Put(Span<byte> buf, ulong offset)
        {
            if (buf == null || buf.IsEmpty)
            {
                return 0;
            }
            _stream.Position = (long)offset; 
            try
            {
                _stream.Write(buf.ToArray(), 0, buf.Length);
                _stream.Flush();
            }
            catch (Exception e)
            {
                throw new IOException($"failed to write data to fileStream,{e.Message}");
            }
            return (ulong)buf.Length;
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
