using Azure.Data.Tables;

namespace ServcoX.EventSauce.Tables;

public sealed class IndexTable(TableClient table)
{
    public void CreateUnderlyingIfNotExist() =>
        table.CreateIfNotExists();

}