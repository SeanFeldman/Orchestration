using Shared;

Console.WriteLine(Assembly.GetExecutingAssembly().GetName().Name);

var connectionString = Environment.GetEnvironmentVariable("AzureServiceBus_ConnectionString");

var client = new ServiceBusClient(connectionString);

var publisher = client.CreateSender("orchestration");

// TODO: ScheduledEnqueueTime doesn't work with Sessions?
// await publisher.SendMessageAsync(new ServiceBusMessage("Shipping OK")
// {
//     SessionId = Correlation.Id,
//     ApplicationProperties = { { "MessageType", "ItemShipped" }  },
//     // will trigger timeout
//     // ScheduledEnqueueTime = DateTimeOffset.Now.Add(TimeSpan.FromSeconds(7)) //seems not to work with sessions
// });

var sequenceNumber = await publisher.ScheduleMessageAsync(new ServiceBusMessage("Shipping OK")
{
    SessionId = Correlation.Id,
    ApplicationProperties = { { "MessageType", "ItemShipped" } },
}, DateTimeOffset.Now.Add(TimeSpan.FromSeconds(7)));

Console.WriteLine("Item shipped");