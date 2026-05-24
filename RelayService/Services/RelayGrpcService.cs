using System.Data.Common;
using Application.Abstractions;
using Application.Contracts;
using Domain;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using RelayService.Protos;

namespace RelayService.Services;

public sealed class RelayGrpcService(
    IRelayApplicationService relayApplicationService,
    ILogger<RelayGrpcService> logger) : Relay.RelayBase
{
    public override Task<EnqueueMessageResponse> EnqueueMessage(EnqueueMessageRequest request, ServerCallContext context) =>
        ExecuteAsync(
            nameof(EnqueueMessage),
            async cancellationToken =>
            {
                var msgId = ParseUuid(request.MsgId, nameof(request.MsgId));
                var destMailbox = ParseUuid(request.DestMailbox, nameof(request.DestMailbox));
                var payload = request.Payload.ToByteArray();

                ValidatePayload(payload);

                var result = await relayApplicationService.EnqueueAsync(msgId, destMailbox, payload, cancellationToken);

                return result.Status switch
                {
                    EnqueueMessageStatus.Accepted => new EnqueueMessageResponse { Accepted = true },
                    EnqueueMessageStatus.DuplicatePayloadMismatch => throw new RpcException(
                        new Status(StatusCode.AlreadyExists, "A different payload already exists for this msg_id.")),
                    _ => throw new RpcException(new Status(StatusCode.Internal, "Internal relay error."))
                };
            },
            context.CancellationToken);

    public override Task<GetMessageResponse> GetMessage(GetMessageRequest request, ServerCallContext context) =>
        ExecuteAsync(
            nameof(GetMessage),
            async cancellationToken =>
            {
                var msgId = ParseUuid(request.MsgId, nameof(request.MsgId));
                var payload = await relayApplicationService.GetMessageAsync(msgId, cancellationToken);

                return payload is null
                    ? throw new RpcException(new Status(StatusCode.NotFound, "Message was not found."))
                    : new GetMessageResponse { Payload = Google.Protobuf.ByteString.CopyFrom(payload) };
            },
            context.CancellationToken);

    public override Task<GetPendingMessagesResponse> GetPendingMessages(GetPendingMessagesRequest request, ServerCallContext context) =>
        ExecuteAsync(
            nameof(GetPendingMessages),
            async cancellationToken =>
            {
                ValidateMailboxCount(request.MailboxIds.Count);

                var mailboxIds = request.MailboxIds
                    .Select(mailboxId => ParseUuid(mailboxId, "mailbox_ids"))
                    .Distinct()
                    .ToArray();

                var limit = ValidatePendingLimit(request.Limit);
                var result = await relayApplicationService.GetPendingMessagesAsync(mailboxIds, limit, cancellationToken);

                var response = new GetPendingMessagesResponse
                {
                    HasMore = result.HasMore
                };

                response.Messages.AddRange(result.Messages.Select(MapPendingMessage));
                return response;
            },
            context.CancellationToken);

    public override Task<AckMessageResponse> AckMessage(AckMessageRequest request, ServerCallContext context) =>
        ExecuteAsync(
            nameof(AckMessage),
            async cancellationToken =>
            {
                var msgId = ParseUuid(request.MsgId, nameof(request.MsgId));
                await relayApplicationService.AckMessageAsync(msgId, cancellationToken);
                return new AckMessageResponse { Success = true };
            },
            context.CancellationToken);

    public override Task<AckMessagesBatchResponse> AckMessagesBatch(AckMessagesBatchRequest request, ServerCallContext context) =>
        ExecuteAsync(
            nameof(AckMessagesBatch),
            async cancellationToken =>
            {
                ValidateAckBatchCount(request.MsgIds.Count);

                var msgIds = request.MsgIds
                    .Select(msgId => ParseUuid(msgId, "msg_ids"))
                    .ToArray();

                var ackedCount = await relayApplicationService.AckMessagesBatchAsync(msgIds, cancellationToken);
                return new AckMessagesBatchResponse { AckedCount = ackedCount };
            },
            context.CancellationToken);

    private async Task<TResponse> ExecuteAsync<TResponse>(
        string operation,
        Func<CancellationToken, Task<TResponse>> action,
        CancellationToken cancellationToken)
    {
        try
        {
            return await action(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (RpcException)
        {
            throw;
        }
        catch (DbException exception)
        {
            logger.LogError(
                "Relay operation failed due to infrastructure availability. Operation: {Operation}, ExceptionType: {ExceptionType}.",
                operation,
                exception.GetType().FullName);

            throw new RpcException(new Status(StatusCode.Unavailable, "Relay dependencies are temporarily unavailable."));
        }
        catch (Exception exception)
        {
            logger.LogError(
                "Relay operation failed. Operation: {Operation}, ExceptionType: {ExceptionType}.",
                operation,
                exception.GetType().FullName);

            throw new RpcException(new Status(StatusCode.Internal, "Internal relay error."));
        }
    }

    private static Guid ParseUuid(string value, string fieldName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, $"{fieldName} is required."));
        }

        if (!Guid.TryParse(value, out var parsedValue) || parsedValue == Guid.Empty)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, $"{fieldName} must be a non-empty UUID."));
        }

        return parsedValue;
    }

    private static int ValidatePendingLimit(int limit)
    {
        if (limit == 0)
        {
            return RelayConstraints.DefaultPendingMessagesLimit;
        }

        return limit is < 1 or > RelayConstraints.MaxPendingMessagesLimit
            ? throw new RpcException(new Status(StatusCode.InvalidArgument, "limit must be between 1 and 500."))
            : limit;
    }

    private static void ValidateMailboxCount(int mailboxCount)
    {
        if (mailboxCount == 0)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "mailbox_ids must contain at least one mailbox."));
        }

        if (mailboxCount > RelayConstraints.MaxMailboxCount)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "mailbox_ids cannot contain more than 7 mailboxes."));
        }
    }

    private static void ValidateAckBatchCount(int count)
    {
        if (count == 0)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "msg_ids must contain at least one msg_id."));
        }

        if (count > RelayConstraints.MaxAckBatchSize)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "msg_ids cannot contain more than 500 entries."));
        }
    }

    private static void ValidatePayload(byte[] payload)
    {
        if (payload.Length == 0)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "payload is required."));
        }

        if (payload.Length > RelayConstraints.MaxPayloadBytes)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "payload exceeds the 256KB limit."));
        }
    }

    private static RelayService.Protos.PendingMessage MapPendingMessage(PendingMessageItem message) =>
        new()
        {
            MsgId = message.MsgId.ToString("D"),
            DestMailbox = message.DestMailbox.ToString("D"),
            Payload = Google.Protobuf.ByteString.CopyFrom(message.Payload)
        };
}
