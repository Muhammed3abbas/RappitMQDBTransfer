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

                    var result = channel.BasicGet("data_queue", autoAck: false);
                    if (result == null)
                    {
                        Console.WriteLine("There are no messages to consume.");
                        return;
                    }

                    // If there are messages, set up a consumer to process them
                    var consumer = new EventingBasicConsumer(channel);
                    bool dataConsumed = false;
                    consumer.Received += async (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);

                        var fields = message.Split(',');
                        if (fields.Length == 2)
                        {
                            var firstName = fields[0];
                            var lastName = fields[1];

                            var success = await InsertEmployeeAsync(firstName, lastName);
                            if (success)
                            {
                                //Console.WriteLine($"Successfully saved Employee: {firstName} {lastName} to the database.");
                                dataConsumed = true;
                            }
                            else
                            {
                                Console.WriteLine($"Failed to save Employee: {firstName} {lastName} to the database.");
                            }

                            channel.BasicAck(ea.DeliveryTag, false);
                        }
                        else
                        {
                            Console.WriteLine("Invalid message format.");
                        }
                    };

                    channel.BasicConsume(queue: "data_queue",
                                         autoAck: false,
                                         consumer: consumer);

                    // Allow some time for messages to be processed
                    await Task.Delay(1000);

                    if (dataConsumed)
                    {
                        Console.WriteLine("Data consumed successfully.");
                    }
                    else
                    {
                        Console.WriteLine("No valid data was consumed.");
                    }
                }
            }

            private async Task<bool> InsertEmployeeAsync(string firstName, string lastName)
            {
                try
                {
                    using (var connection = new SqlConnection(_dbConnectionString))
                    {
                        string sql = "INSERT INTO Employees (FirstName, LastName) VALUES (@FirstName, @LastName)";
                        await connection.ExecuteAsync(sql, new { FirstName = firstName, LastName = lastName });
                        return true;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error inserting employee: {ex.Message}");
                    return false;
                }
            }

            public void Dispose()
            {
                // Dispose resources if any
            }
        }
    }


