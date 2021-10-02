using System;
using System.Collections.Generic;
using System.Text;

namespace Storage
{
    public class Test
    {
        int _intData;
        string _stringData;

        public int Int
        {
            set
            {
                _intData = value;
            }
            get
            {
                return (_intData);
            }
        }

        public string String
        {
            set
            {
                _stringData = value;
            }
            get
            {
                return (_stringData);
            }
        }

        // Method 

        public byte[] Serialise()
        {
            byte[] b = new byte[10];
            return (b);

        }

        public override string ToString()
        {
            return ("Int=" + _intData + " String=" + _stringData);
        }

    }
}
