using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcDB.Core
{
    public class Iterator : IEnumerable<(byte[], byte[])>,IEnumerator
    {
        public Iterator(List<(byte[], byte[])> items)
        {
            this._items = items;
        }
        private List<(byte[], byte[])> _items;
        private int _current_index = 0;

        public object Current => _items[_current_index];

        public (byte[]?, byte[]?) Next()
        {
            if (_current_index >= _items.Count)
                return (null,null);
            var result  =_items[_current_index];
            _current_index++;
            return result;
        }

        public IEnumerator<(byte[], byte[])> GetEnumerator()
        {
            return this._items.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this;
        }

        public bool MoveNext()
        {
            var (key,value)=this.Next();
            if(key == null || value == null)
                return false;
            return true;
        }

        public void Reset()
        {
            _current_index = 0;
        }
    }
}
