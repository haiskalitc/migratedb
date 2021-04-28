using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MigrateDB
{
    class MainClass
    {
        /// <summary>
        /// Getdata
        /// </summary>
        /// <param name="args"></param>
        public static void Main(string[] args)
        {
            Task.Run(() =>
            {
                Worker.Runner();
            });

            Console.ReadLine();
        }
    }
}