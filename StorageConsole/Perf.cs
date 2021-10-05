using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using StorageLibrary;

namespace StorageConsole
{
    class Perf
    {
        static void Main(string[] args)
        {
            
            int records = 1000;
            SequentialTest(records);

            //records = 10000;
            //SequentialTest(records);

            //records = 100000;
            //SequentialTest(records);

        }

        public static void SequentialTest(int records)
        {
            Storage<TestStruct> storage = new Storage<TestStruct>(true);
            Console.WriteLine("Records={0}", records);
            TestStruct ts;
            ts = new TestStruct();
            //ts.Int = i;
            ts.Chars = new char[] { 'g', 'o', 'o', 'd', 'b', 'y' };

            Stopwatch sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < records; i++)
            {
                storage.Handler.Create(ts.ToByteArray());
            }
            sw.Stop();
            Console.WriteLine("Created {0} records in {1}", records, sw.Elapsed);
            Console.WriteLine("Records per second {0}", records / sw.Elapsed.TotalSeconds);

            sw.Start();
            TestStruct d = new TestStruct();
            for (int i = 0; i < storage.Size; i++)
            {
                d = storage.Read(i);           // Read a record
            }
            sw.Stop();
            Console.WriteLine("Read {0} records in {1}", records, sw.Elapsed);
            Console.WriteLine("Records per second {0}", storage.Size / sw.Elapsed.TotalSeconds);

            sw.Start();
            foreach (TestStruct e in storage)
            {
                //
            }
            sw.Stop();
            Console.WriteLine("ForEach Read {0} records in {1}", records, sw.Elapsed);
            Console.WriteLine("Records per second {0}", storage.Size / sw.Elapsed.TotalSeconds);

        }



    }
}