using System;
using System.Collections.Generic;
using System.Text;

namespace StorageConsole
{
    interface ICommand
    {
        // Have a differnt vaming
        // Argument
        //   


        // options Argument sub-commands
        // 
        // rootCommand
        //   Argument
        //     Option=value
        //     Option=value
        //   sub-command
        //     Argument
        //       Option=value
        //       Option=value


        bool ValidateArguments(string[] args);
        string Usage();
        bool PreProcess(string[] args, out List<Parameter> parameters);
        int PostProcess(List<Parameter> parameters);

    }
}
