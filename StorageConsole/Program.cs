using System;
using StorageLibrary;
using System.CommandLine;
using System.CommandLine.Parser;
using System.CommandLine.Invocation;

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
        // and log to the console all the data in cvs format

        #region Fields

        string _name = "";
        string _path = "";

        #endregion

        static int Main(string[] args)
        {
            var root = new RootCommand()
            {
                 new Option<int>(new string[] { "--name", "-N" }, description: "The databse name"),
                 new Option<int>(new string[] { "--path", "-P" }, description: "The databse path"),
            };

            Command set = new Command("set", "Set the field")
            {

            };
            set.Handler = CommandHandler.Create<int>(HandleSet);

            Command delete = new Command("delete", "Delete record by index")
            {
                new Argument<int>("index","Record index")
            };
            delete.Handler = CommandHandler.Create<int>(HandleDelete);

            Command update = new Command("update", "Update record by index")
            {
                new Option<int>(new string[] { "--index", "-I" }, description: "The record index"),
                new Option(new string[] { "--all", "-A" }, description: "All records"),
            };
            update.Handler = CommandHandler.Create<int, bool>(HandleUpdate);

            root.Add(delete);
            root.Add(update);

            return root.Invoke(args);
        }

        private static void HandleDelete(int index)
        {
            //throw new NotImplementedException();
        }

        private static void HandleUpdate(int index, bool all)
        {
            //throw new NotImplementedException();
        }

        private static void HandleSet(int index)
        {
            // Field name
            // Field type
            // Field length if string


            //throw new NotImplementedException();
        }

    }
}
