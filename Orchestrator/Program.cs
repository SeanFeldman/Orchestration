WriteLine(Assembly.GetExecutingAssembly().GetName().Name);

//var connectionString = Environment.GetEnvironmentVariable("AzureServiceBus_ConnectionString");
var connectionString = "Endpoint=sb://seanfeldman-test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Y8jOv/ZUzbdVWpFP+/MR/5o2e3hCpMJMtWGjdo17ucw=";

var client = new ServiceBusClient(connectionString);

var options = new ServiceBusSessionProcessorOptions
{
    // for easy demo
    MaxConcurrentSessions = 1,
    MaxConcurrentCallsPerSession = 1,
    SessionIdleTimeout = TimeSpan.FromSeconds(15)
};
var processor = client.CreateSessionProcessor(topicName: "orchestration", subscriptionName: "orchestrator", options);

processor.SessionInitializingAsync += args =>
{
    WriteLine($"Handling session with ID: {args.SessionId}");
    return Task.CompletedTask;
};

processor.SessionClosingAsync += async args =>
{
    WriteLine($"Closing session with ID: {args.SessionId}");

    var sessionState = await args.GetSessionStateAsync();
    if (sessionState is not null)
    {
        var state = sessionState.ToObject<State>(new JsonObjectSerializer())!;

        if (state.Completed)
        {
            await args.SetSessionStateAsync(null);
        }
    }
};

processor.ProcessErrorAsync += args =>
{
    // TODO: no session ID?
    WriteLine($"Error: {args.Exception}", warning: true);
    return Task.CompletedTask;
};

processor.ProcessMessageAsync += async args =>
{
    var message = args.Message;
    var messageType = message.ApplicationProperties["MessageType"];
    WriteLine($"Got a message of type: {messageType} for session with ID {args.SessionId}");

    var sessionState = await args.GetSessionStateAsync();
    var state = sessionState is null
        ? new State {RetryIn = TimeSpan.FromSeconds(20), TotalRetries = 2 }
        : sessionState.ToObject<State>(new JsonObjectSerializer())!;

    if (state.Completed)
    {
        WriteLine($"Completing the process for Order with correlation ID {message.SessionId}");

        var publisher = client.CreateSender("orchestration");
        await publisher.SendMessageAsync(new ServiceBusMessage($"Orchestration for Order with session ID {message.SessionId} is completed"));
    }

    Func<State, Task> ExecuteAction = messageType switch
    {
        "PaymentAccepted" => async delegate
        {
            state.PaymentReceived = true;
            await SetTimeout(client, args, state);
        },
        "ItemShipped" => async delegate
        {
            state.ItemShipped = true;
            await SetTimeout(client, args, state);
        },
        "Timeout" => async delegate
        {
            if (state.Completed || sessionState is null) // sessionState is null when timeout arrives after orchestration is completed
            {
                WriteLine($"Orchestration ID {args.SessionId} has completed. Discarding timeout.");
                return;
            }

            if (state.ShouldRetry())
            {
                await SetTimeout(client, args, state);
            }
            else
            {
                WriteLine($"Exhausted all retries ({state.TotalRetries}), executing compensating action and completing session with ID {args.SessionId}", warning: true);
                // Compensating action here
                await args.SetSessionStateAsync(null);
            }
        },
        _ => throw new Exception($"Received unexpected message type {messageType} (message ID: {message.MessageId})")
    };

    await ExecuteAction(state);

    if (state.Completed)
    {
        WriteLine($"Removing state for session ID {args.SessionId}");
        await args.SetSessionStateAsync(null);
    }

    static async Task SetTimeout(ServiceBusClient client, ProcessSessionMessageEventArgs args, State state)
    {
        if (state.Completed)
        {
            WriteLine($"Orchestration with session ID {args.SessionId} has completed.");
            await args.SetSessionStateAsync(BinaryData.FromObjectAsJson(state));

            return;
        }

        WriteLine($"Scheduling a timeout to check in {state.RetryIn}");

        var publisher = client.CreateSender("orchestration");
        await publisher.ScheduleMessageAsync(new ServiceBusMessage
        {
            SessionId = args.Message.SessionId,
            ApplicationProperties = { { "MessageType", "Timeout" } }
        }, DateTimeOffset.Now.Add(state.RetryIn));

        state.Retried();

        await args.SetSessionStateAsync(BinaryData.FromObjectAsJson(state));
    }
};

await processor.StartProcessingAsync();

WriteLine("Press Enter to stop Orchestrator");
Console.ReadLine();

await processor.StopProcessingAsync();

await client.DisposeAsync();

static void WriteLine(string value, bool warning = false)
{
    Console.ForegroundColor = ConsoleColor.Cyan;
    Console.Write($"[{DateTime.Now:HH:mm:ss}] ");

    ((Action)(warning ? () => Console.ForegroundColor = ConsoleColor.DarkYellow : Console.ResetColor))();

    Console.WriteLine(value);
    Console.ResetColor();
}

// Mention MT and NSB (https://docs.particular.net/tutorials/nservicebus-sagas/3-integration/)