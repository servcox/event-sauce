using System.Globalization;
using System.Text.Json.Serialization;

namespace ServcoX.EventSauce;

public static class EventStream
{
    /*
     * Event records look like this:
     * {at}\t{eventType}\t{eventPayload}\n
     */

    private const String DateFormatString = @"yyyyMMdd\THHmmssK";
    private const Char FieldSeparator = '\t';
    private const Char RecordSeparator = '\n';
    private static readonly Byte[] FieldSeparatorBytes = { Convert.ToByte(FieldSeparator) };
    private static readonly Byte[] RecordSeparatorBytes = { Convert.ToByte(RecordSeparator) };

    private static readonly JsonSerializerOptions SerializationOptions = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingDefault,
    };

    public static MemoryStream Encode(IEnumerable<IEvent> events, DateTime at)
    {
        ArgumentNullException.ThrowIfNull(events);

        var stream = new MemoryStream();
        foreach (var evt in events)
        {
            if (evt is null) throw new BadEventException("One or more events are null");

            var formattedAt = at.ToString(DateFormatString, CultureInfo.InvariantCulture);
            var eventType = new EventType(evt.GetType());
            var payloadEncoded = JsonSerializer.Serialize((Object)evt, SerializationOptions); // Cast as object, otherwise STJ will only serialize what it sees on IEvent (ie, no fields)

            stream.WriteAsUtf8(formattedAt);
            stream.Write(FieldSeparatorBytes);
            stream.WriteAsUtf8(eventType.Name);
            stream.Write(FieldSeparatorBytes);
            stream.WriteAsUtf8(payloadEncoded);
            stream.Write(FieldSeparatorBytes);
            stream.Write(RecordSeparatorBytes);
        }

        stream.Rewind();
        return stream;
    }

    public static List<EventRecord> Decode(Stream stream)
    {
        var output = new List<EventRecord>();

        var reader = new StreamLineReader(stream);
        while (reader.TryReadLine() is { } line)
        {
            var length = line.Length;
            if (length == 0) continue; // Skip blank lines - used in testing
            var startPosition = reader.Position - length;

            var tokens = line.Split(FieldSeparator);
            if (tokens.Length != 3) throw new BadEventException("Event does not have exactly three tokens");

            var at = DateTime.ParseExact(tokens[0], DateFormatString, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);
            var eventType = new EventType(tokens[1]);
            var deserializationType = eventType.TryDecode() ?? typeof(Object);
            var evt = JsonSerializer.Deserialize(tokens[2], deserializationType, SerializationOptions) ?? throw new NeverException();

            output.Add(new
            (
                at,
                eventType,
                evt,
                startPosition,
                length
            ));
        }

        return output;
    }
}