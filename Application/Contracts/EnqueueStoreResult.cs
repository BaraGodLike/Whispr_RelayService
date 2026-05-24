namespace Application.Abstractions;

public enum EnqueueStoreResult
{
    Inserted = 0,
    DuplicateWithSamePayload = 1,
    DuplicateWithDifferentPayload = 2
}
