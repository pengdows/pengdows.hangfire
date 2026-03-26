using Hangfire.Storage;

namespace pengdows.hangfire.contracts;

/// <summary>
/// Our interface seam over IWriteOnlyTransaction.
/// If Hangfire adds or removes a member from IWriteOnlyTransaction, this interface
/// will fail to compile, catching the break before it reaches the concrete class.
/// </summary>
public interface IHangfireTransaction : IWriteOnlyTransaction { }
