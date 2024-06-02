using Dapper;
using Microsoft.Data.SqlClient;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading.Tasks;



namespace RabbitMQDBTransfer
{
    public class RabbitMQConsumer : IDisposable
    {
        private readonly ConnectionFactory _connectionFactory;
        private readonly string _dbConnectionString;

        public RabbitMQConsumer(string rabbitMQConnectionString, string dbConnectionString)
        {
            _connectionFactory = new ConnectionFactory() { Uri = new Uri(rabbitMQConnectionString) };
            _dbConnectionString = dbConnectionString;
        }

        public async Task StartListeningAsync()
        {
            using (var connection = _connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "data_queue",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += async (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);

                    var fields = message.Split(',');
                    if (fields.Length == 2)
                    {
                        var firstName = fields[0];
                        var lastName = fields[1];

                        await InsertEmployeeAsync(firstName, lastName);
                    }
                    else
                    {
                        Console.WriteLine("Invalid message format.");
                    }

                    channel.BasicAck(ea.DeliveryTag, false);
                };

                channel.BasicConsume(queue: "data_queue",
                                     autoAck: false,
                                     consumer: consumer);

                Console.WriteLine("Listening for messages...");
                await Task.Delay(5000); // Simulate a 5-second delay for receiving messages

                Console.WriteLine("Data consumed successfully");
            }
        }

        private async Task InsertEmployeeAsync(string firstName, string lastName)
        {
            try
            {
                using (var connection = new SqlConnection(_dbConnectionString))
                {
                    string sql = "INSERT INTO Employees (FirstName, LastName) VALUES (@FirstName, @LastName)";
                    await connection.ExecuteAsync(sql, new { FirstName = firstName, LastName = lastName });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error inserting employee: {ex.Message}");
            }
        }

        public void Dispose()
        {
            // Dispose resources if any
        }
    }
}

