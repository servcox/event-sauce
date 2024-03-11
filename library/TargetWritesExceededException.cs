namespace ServcoX.EventSauce;

public class TargetWritesExceededException : Exception
{
    public TargetWritesExceededException()
    {
    }

    public TargetWritesExceededException(String message) : base(message)
    {
    }

    public TargetWritesExceededException(String message, Exception innerException) : base(message, innerException)
    {
    }
}