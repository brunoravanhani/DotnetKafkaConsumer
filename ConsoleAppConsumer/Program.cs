using System;

namespace ConsoleAppConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Consumming user-message");
            var consumer = new Consumer("localhost:9092", "user-message-group");
            consumer.Consume("user-message");
        }
    }
}
