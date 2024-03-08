namespace ServcoX.EventSauce.V3.Exceptions;

public class TransactionTooLargeException : Exception
{
    public TransactionTooLargeException()
    {
    }

    public TransactionTooLargeException(String message) : base(message)
    {
    }

    public TransactionTooLargeException(String message, Exception innerException) : base(message, innerException)
    {
    }
}