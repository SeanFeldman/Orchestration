class State
{
    public bool PaymentReceived { get; set; }
    public bool ItemShipped { get; set; }
    public int RetriesCount { get; set; }
    public bool Completed => PaymentReceived && ItemShipped;
}