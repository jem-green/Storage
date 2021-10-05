using System;
using System.Data;
using StorageLibrary;

namespace StorageConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            PersistentStorage<TestClass> storage = new PersistentStorage<TestClass>(true);

            // Create test class

            TestClass t = new TestClass
            {
                Int = 0,
                String = "hello"
            };
            storage.Create(t);           // Create a new records
            TestClass d;
            for (int i = 0; i < storage.Size; i++)
            {
                d = storage.Read(i);
                Console.WriteLine("create = [" + d + "}");
            }

            // Update test shorter

            TestClass u = new TestClass
            {
                Int = 1,
                String = "hi"
            };
            storage.Update(u, 0);       // Update some data
            for (int i = 0; i < storage.Size; i++)
            {
                d = storage.Read(i);
                Console.WriteLine("shorter = [" + d + "}");
            }

            // Read test
            // Add another record and check both are listed

            t = new TestClass
            {
                Int = 2,
                String = "welcome"
            };
            storage.Create(t);           // Create a new records

            for (int i = 0; i < storage.Size; i++)
            {
                d = storage.Read(i);
                Console.WriteLine("mutiple = [" + d + "}");
            }

            // Delete test
            // Check that the second record is returned

            storage.Delete(0);          // Delete some data
            for (int i = 0; i < storage.Size; i++)
            {
                d = storage.Read(i);
                Console.WriteLine("delete = [" + d + "}");
            }

            // create more data to check effect of delete

            t = new TestClass
            {
                Int = 3,
                String = "bonjour"
            };
            storage.Create(t);           // Create a new records
            for (int i = 0; i < storage.Size; i++)
            {
                d = storage.Read(i);
                Console.WriteLine("post create = [" + d + "}");
            }

            // Update test longer

            u = new TestClass
            {
                Int = 4,
                String = "konnichiwa"
            };
            storage.Update(u, 0);       // Update some data
            for (int i = 0; i < storage.Size; i++)
            {
                d = storage.Read(i);
                Console.WriteLine("longer = [" + d + "}");
            }

            // Test for each

            foreach (TestClass e in storage)
            {
                Console.WriteLine("foreach = [" + e + "}");
            }

        }
    }
}

