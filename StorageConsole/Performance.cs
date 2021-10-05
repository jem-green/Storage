using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using StorageLibrary;

namespace StorageConsole
{
    class Performance
    {
        static void Main(string[] args)
        {
            /*
             * Records=1000
                Created 1000 records in 00:00:07.6737890
                Records per second 130.3137211617364
                Read 1000 records in 00:00:07.7946921
                Records per second 128.29243120456292
                Records=10000
                Created 10000 records in 00:01:26.4067695
                Records per second 115.73167308378541
                Read 1000 records in 00:01:27.3785493
                Records per second 114.44456425645876
                Records=100000
                Created 100000 records in 00:15:36.9666700
                Records per second 106.72738230912739
                Read 1000 records in 00:15:42.7973934
                Records per second 36.555043789114485
             */

            int records = 1000;
            SequentialTest(records);

            //records = 10000;
            //SequentialTest(records);

            //records = 100000;
            //SequentialTest(records);

            //records = 1000;
            //RandomTest(records);

            //records = 10000;
            //RandomTest(records);

            //records = 100000;
            //RandomTest(records);

        }

        public static void RandomTest(int records)
        {
            PersistentStorage<TestClass> storage = new PersistentStorage<TestClass>(true);
            Console.WriteLine("Records={0}", records);

            // Create an array of Tests

            int[] items = new int[records];
            for (int i = 0; i < records; i++)
            {
                items[i] = i;
            }

            // Shuffle the list using Fisher-Yates method

            Random rng = new Random();
            int n = items.Length;
            while (n > 1)
            {
                n--;
                int k = rng.Next(n + 1);
                int value = items[k];
                items[k] = items[n];
                items[n] = value;
            }

            TestClass t;
            Stopwatch sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < records; i++)
            {
                t = new TestClass
                {
                    Int = i,
                    String = "hello"
                };
                storage.Create(t);           // Create a new records
            }
            sw.Stop();
            Console.WriteLine("Created {0} records in {1}", records, sw.Elapsed);
            Console.WriteLine("Records per second {0}", records / sw.Elapsed.TotalSeconds);

            sw.Start();
            TestClass d = new TestClass();
            for (int i = 0; i < storage.Size; i++)
            {
                d = storage.Read(items[i]);           // Read a record
            }
            sw.Stop();
            Console.WriteLine("Random Read {0} records in {1}", records, sw.Elapsed);
            Console.WriteLine("Records per second {0}", storage.Size / sw.Elapsed.TotalSeconds);
        }

        public static void SequentialTest(int records)
        {
            PersistentStorage<TestClass> storage = new PersistentStorage<TestClass>(true);
            Console.WriteLine("Records={0}", records);
            TestClass t;
            Stopwatch sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < records; i++)
            {
                t = new TestClass
                {
                    Int = i,
                    String = "hello"
                };
                storage.Create(t);           // Create a new records
            }
            sw.Stop();
            Console.WriteLine("Created {0} records in {1}", records, sw.Elapsed);
            Console.WriteLine("Records per second {0}", records / sw.Elapsed.TotalSeconds);

            sw.Start();
            TestClass d = new TestClass();
            for (int i = 0; i < storage.Size; i++)
            {
                d = storage.Read(i);           // Read a record
            }
            sw.Stop();
            Console.WriteLine("Read {0} records in {1}", records, sw.Elapsed);
            Console.WriteLine("Records per second {0}", storage.Size / sw.Elapsed.TotalSeconds);

            sw.Start();
            foreach (TestClass e in storage)
            {
                //
            }
            sw.Stop();
            Console.WriteLine("ForEach Read {0} records in {1}", records, sw.Elapsed);
            Console.WriteLine("Records per second {0}", storage.Size / sw.Elapsed.TotalSeconds);

        }



    }
}