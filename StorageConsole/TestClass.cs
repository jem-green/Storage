using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.InteropServices;

namespace StorageConsole
{
    public class TestClass
    {
        #region Variables

        int _intData;
        string _stringData;

        #endregion
        #region Properties

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

        #endregion
        #region Methods

        // Method 

        public override string ToString()
        {
            return ("Int=" + _intData + " String=" + _stringData);
        }

        #endregion

    }

}
