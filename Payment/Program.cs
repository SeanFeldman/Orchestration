using Shared;

Console.WriteLine(Assembly.GetExecutingAssembly().GetName().Name);

//var connectionString = Environment.GetEnvironmentVariable("AzureServiceBus_ConnectionString");
var connectionString = "Endpoint=sb://seanfeldman-test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Y8jOv/ZUzbdVWpFP+/MR/5o2e3hCpMJMtWGjdo17ucw=";

var client = new ServiceBusClient(connectionString);

var publisher = client.CreateSender("orchestration");

// Emulate missing event
await publisher.SendMessageAsync(new ServiceBusMessage("Payment OK")
{
    SessionId = Correlation.Id,
    ApplicationProperties = { { "MessageType", "PaymentAccepted" }  }
});

Console.WriteLine("Payment accepted");