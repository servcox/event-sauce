namespace ServcoX.EventSauce;

public class SegmentFullException : Exception
{
    public SegmentFullException()
    {
    }

    public SegmentFullException(String message) : base(message)
    {
    }

    public SegmentFullException(String message, Exception innerException) : base(message, innerException)
    {
    }
}