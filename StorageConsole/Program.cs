using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using StorageLibrary;

namespace StorageConsole
{
    /// <summary>
    /// Simple command line to return results
    /// More like the docker command line so
    /// not a query language just parameters
    /// </summary>
    class Program
    {
        // Need the name and location of the file to read
        // could pass this as a single parameter if nothing
        // else is supplied
        // so --name --path
        // and log to the console all the data in CSV format

        #region Fields

        static bool _readInput = false;
        static Version _version;
        static RootCommand _arguments;
        static List<Parameter> _parameters;

        #endregion

        static int Main(string[] args)
        {

            int errorCode = 0;
            _arguments = new RootCommand();
            _parameters = new List<Parameter>();

            if (_arguments.ValidateArguments(args))
            {
                _readInput = _arguments.PreProcess(args, out _parameters);
                if (_readInput)
                {
                    string currentLine = Console.In.ReadLine();
                    while (currentLine != null)
                    {
                        //ProcessLine(currentLine);
                        currentLine = Console.In.ReadLine();
                    }
                }
                errorCode = _arguments.PostProcess(_parameters);
            }
            else
            {
                Console.Error.Write(_arguments.Usage());
                errorCode = -1;
            }
            return (errorCode);
        }

        static int Create()
        {
            int errorCode = -1;

            return (errorCode);
        }

    }
}
