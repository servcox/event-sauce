namespace ServcoX.EventSauce;

public class EventParseException : Exception
{
    public EventParseException()
    {
    }

    public EventParseException(String message) : base(message)
    {
    }

    public EventParseException(String message, Exception innerException) : base(message, innerException)
    {
    }
}