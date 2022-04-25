class State
{
    public bool PaymentReceived { get; set; }
    public bool ItemShipped { get; set; }
    public TimeSpan RetryIn { get; init; }
    public int RetriesCount { get; /*internal*/ set; }
    public int TotalRetries { get; init; }
    public bool Completed => PaymentReceived && ItemShipped;

    public bool ShouldRetry() => RetriesCount < TotalRetries;
    public void Retried() => RetriesCount++;
}