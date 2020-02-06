using System;
using System.Threading.Tasks;

namespace ConsoleAppKafka
{
    class Program
    {
        static void Main(string[] args)
        {

            var producer = new Producer("localhost:9092");
            

            bool exit = false;

            do {
                Console.WriteLine("Send message: ");
                var message = Console.ReadLine();

                if (message == "exit")
                    break;

                producer.Send("user-message", message);

            } while (!exit);


        }
    }
}
