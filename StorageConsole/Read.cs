using System;
using System.Collections.Generic;
using System.Text;

namespace StorageConsole
{
    class Read : ICommand
    {
        int _index = -1;
        List<Parameter> _parameters;

        #region Constructors

        public Read(int index, List<Parameter> parameters)
        {
            _index = index;
            _parameters = parameters;
        }

        #endregion

        public int PostProcess(List<Parameter> parameters)
        {
            throw new NotImplementedException();
        }

        public bool PreProcess(string[] args, out List<Parameter> parameters)
        {
            throw new NotImplementedException();
        }

        public string Usage()
        {
            throw new NotImplementedException();
        }

        public bool ValidateArguments(string[] args)
        {
            throw new NotImplementedException();
        }
    }
}
