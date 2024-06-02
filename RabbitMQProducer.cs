using Dapper;
using Microsoft.Data.SqlClient;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;


namespace RabbitMQDBTransfer
{
    public class RabbitMQProducer : IDisposable
    {
        private readonly ConnectionFactory _connectionFactory;
        private readonly string _dbConnectionString;

        public RabbitMQProducer(string rabbitMQConnectionString, string dbConnectionString)
        {
            _connectionFactory = new ConnectionFactory() { Uri = new Uri(rabbitMQConnectionString) };
            _dbConnectionString = dbConnectionString;
        }

        public async Task SendEmployeeDataAsync()
        {
            var employees = await RetrieveEmployeesAsync();

            using (var connection = _connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "data_queue",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                foreach (var employee in employees)
                {
                    string message = $"{employee.FirstName},{employee.LastName}";
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(exchange: "",
                                         routingKey: "data_queue",
                                         basicProperties: null,
                                         body: body);
                }
                Console.WriteLine("Data produced successfully");
            }
        }

        private async Task<IEnumerable<Employee>> RetrieveEmployeesAsync()
        {
            using (var connection = new SqlConnection(_dbConnectionString))
            {
                string sql = "SELECT FirstName, LastName FROM Employees";
                return await connection.QueryAsync<Employee>(sql);
            }
        }

        public void Dispose()
        {
            // Dispose resources if any
        }
    }
}

public class Employee
{
    public string FirstName { get; set; }
    public string LastName { get; set; }
}

