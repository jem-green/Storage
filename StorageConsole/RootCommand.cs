using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace StorageConsole
{
    public class RootCommand : ICommand
    {
        public bool ValidateArguments(string[] args)
        {
            // Check the syntax of the argument structure
            // this is more complex is its the order of the arguments
            // that is important and there is separate help per command
            // Suspect need some sort of hierarchical structure 

            bool help = false;

            for (int item = 0; item < args.Length; item++)
            {
                string lookup = args[item];
                if (!lookup.StartsWith('/') == true)
                {
                    lookup = lookup.ToLower();  // avoid case problems
                }
                switch (lookup)
                {
                    case "/h":
                    case "--help":
                        {
                            help = true;
                            break;
                        }
                }
            }

            if (help == false)
            {
                return (true);
            }
            else
            {
                return (false);
            }
        }

        public string Usage()
        {
            string usage = "";
            usage = "Usage:  storage [OPTIONS] COMMAND\n";
            usage += "\n";
            usage += "A type based storage model\n";
            usage += "\n";
            usage += "Options:\n";
            usage += "  /h|--help         Display help.\n";
            usage += "  /v|--version      Print version information and quit.\n";
            usage += "\n";
            usage += "Commands:\n";
            usage += "  create\n";
            usage += "  read\n";
            usage += "  update\n";
            usage += "  delete\n";
            usage += "\n";
            usage += "Run 'storage COMMAND --help' for more information on a command.\n";
            return (usage);
        }

        public bool PreProcess(string[] args, out List<Parameter> parameters)
        {
            // Not sure we need anything here

            bool readInput = false; // Indicate that we don't need to do a read input
            parameters = new List<Parameter>();
            Parameter parameter;
            int index = -1;

            for (int item = 0; item < args.Length; item++)
            {
                string lookup = args[item];
                if (!lookup.StartsWith('/') == true)
                {
                    lookup = lookup.ToLower();  // avoid case problems
                }

                switch (lookup)
                {
                    case "/v":
                    case "--version":
                        { 
                            parameter = new Parameter("version",true);
                            parameters.Add(parameter);
                            break;
                        }
                    case "/h":
                    case "--help":
                        {
                            parameter = new Parameter("help",true);
                            parameters.Add(parameter);
                            break;
                        }
                    case "create":
                        {
                            if (index < 0)
                            {
                                index = item;
                                try
                                {
                                    parameter = new Parameter("create", index);
                                    parameters.Add(parameter);
                                }
                                catch
                                { }
                            }
                            break;
                        }
                    case "read":
                        {
                            if (index < 0)
                            {
                                index = item;
                                try
                                {
                                    parameter = new Parameter("read",index);
                                    parameters.Add(parameter);
                                }
                                catch
                                { }
                            }
                            break;
                        }
                    case "update":
                        {
                            if (index < 0)
                            {
                                index = item;
                                try
                                {
                                    parameter = new Parameter("update",index);
                                    parameters.Add(parameter);
                                }
                                catch
                                { }
                            }
                            break;
                        }
                    case "delete":
                        {
                            if (index < 0)
                            {
                                index = item;
                                try
                                {
                                    parameter = new Parameter("delete",index);
                                    parameters.Add(parameter);
                                }
                                catch
                                { }
                            }
                            break;
                        }
                }
            }
            return (readInput);
        }

        public int PostProcess(List<Parameter> parameters)
        {
            int errorCode = -1;

            for (int item = 0; item < parameters.Count; item++)
            {
                Parameter parameter = parameters[item];
                switch (parameter.Key)
                {
                    case "version":
                        {
                            Version version = Assembly.GetEntryAssembly().GetName().Version;
                            Console.Out.WriteLine(" Version " + version.Major + "." + version.Minor);
                            errorCode = 0;
                            break;
                        }
                    case "create":
                        {
                            //Create(item, parameters);
                            break;
                        }
                    case "read":
                        {
                            //Read();
                            break;
                        }
                    case "update":
                        {
                            //Update();
                            break;
                        }
                    case "delate":
                        {
                            //Delete();
                        }
                        break;
                }
            }

            return (errorCode);
        }


    }
}
