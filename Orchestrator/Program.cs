WriteLine(Assembly.GetExecutingAssembly().GetName().Name);

var connectionString = Environment.GetEnvironmentVariable("AzureServiceBus_ConnectionString");

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
        ? new State()
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
            await SetTimeoutIfNecessary(client, args, state, TimeSpan.FromSeconds(5));
        },
        "ItemShipped" => async delegate
        {
            state.ItemShipped = true;
            await SetTimeoutIfNecessary(client, args, state, TimeSpan.FromSeconds(5));
        },
        "Timeout" => async delegate
        {
            if (state.Completed || sessionState is null)
            {
                WriteLine($"Orchestration ID {args.SessionId} has completed. Discarding timeout.");
                return;
            }
            
            if (state.RetriesCount < 3)
            {
                await SetTimeoutIfNecessary(client, args, state, TimeSpan.FromSeconds(5));
            }
            else
            {
                WriteLine($"Exhausted all retries ({state.RetriesCount}). Executing compensating action and completing session with ID {args.SessionId}", warning: true);
                // Compensating action here
                await args.SetSessionStateAsync(null);
            }
        },
        _ => throw new Exception($"Received unexpected message type {messageType} (message ID: {message.MessageId})")
    };

    await ExecuteAction(state);

    static async Task SetTimeoutIfNecessary(ServiceBusClient client, ProcessSessionMessageEventArgs args, State state, TimeSpan timeout)
    {
        if (state.Completed)
        {
            WriteLine($"Orchestration with session ID {args.SessionId} has successfully completed. Sending notification (TBD).");
            await args.SetSessionStateAsync(null);
            return;
        }

        WriteLine($"Scheduling a timeout to check in {timeout}");

        var publisher = client.CreateSender("orchestration");
        await publisher.ScheduleMessageAsync(new ServiceBusMessage
        {
            SessionId = args.Message.SessionId,
            ApplicationProperties = { { "MessageType", "Timeout" } }
        }, DateTimeOffset.Now.Add(timeout));

        state.RetriesCount++;
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