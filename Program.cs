using RabbitMQDBTransfer;
using System;
using System.Threading.Tasks;

class Program
{
    private static readonly string producerConnectionString = "Server=.;Database=ProducerDB;Trusted_Connection=True;TrustServerCertificate=True;";
    private static readonly string consumerConnectionString = "Server=.;Database=ConsumerDB;Trusted_Connection=True;TrustServerCertificate=True;";
    private static readonly string rabbitMQConnectionString = "amqp://guest:guest@localhost:5672/";

    static async Task Main(string[] args)
    {
        while (true)
        {
            Console.WriteLine("Choose an option:");
            Console.WriteLine("1. Send employee data");
            Console.WriteLine("2. Start listening for messages");
            Console.WriteLine("3. Exit");

            var choice = Console.ReadLine();

            switch (choice)
            {
                case "1":
                    var producer = new RabbitMQProducer(rabbitMQConnectionString, producerConnectionString);
                    await producer.SendEmployeeDataAsync();
                    break;
                case "2":
                    var consumer = new RabbitMQConsumer(rabbitMQConnectionString, consumerConnectionString);
                    await consumer.StartListeningAsync();
                    break;
                case "3":
                    return;
                default:
                    Console.WriteLine("Invalid choice");
                    break;
            }
        }
    }
}
