using FluentMigrator;

namespace Migrator.Migrations;

[Migration(202605241401)]
public sealed class InitialRelaySchemaMigration : Migration
{
    public override void Up()
    {
        Create.Table("pending_messages")
            .WithColumn("msg_id").AsGuid().PrimaryKey()
            .WithColumn("dest_mailbox").AsGuid().NotNullable()
            .WithColumn("payload").AsBinary(int.MaxValue).NotNullable()
            .WithColumn("created_at").AsDateTimeOffset().NotNullable().WithDefault(SystemMethods.CurrentUTCDateTime);

        Create.Index("ix_pending_messages_dest_mailbox_created_at")
            .OnTable("pending_messages")
            .OnColumn("dest_mailbox").Ascending()
            .OnColumn("created_at").Ascending();

        Create.Table("outbox_events")
            .WithColumn("event_id").AsGuid().PrimaryKey()
            .WithColumn("event_type").AsString(255).NotNullable()
            .WithColumn("msg_id").AsGuid().NotNullable()
            .WithColumn("dest_mailbox").AsGuid().NotNullable()
            .WithColumn("created_at").AsDateTimeOffset().NotNullable().WithDefault(SystemMethods.CurrentUTCDateTime)
            .WithColumn("published").AsBoolean().NotNullable().WithDefaultValue(false)
            .WithColumn("published_at").AsDateTimeOffset().Nullable();

        Create.Index("ix_outbox_events_published_created_at")
            .OnTable("outbox_events")
            .OnColumn("published").Ascending()
            .OnColumn("created_at").Ascending();

        Create.Index("ux_outbox_events_event_type_msg_id")
            .OnTable("outbox_events")
            .OnColumn("event_type").Ascending()
            .OnColumn("msg_id").Ascending()
            .WithOptions().Unique();
    }

    public override void Down()
    {
        Delete.Table("outbox_events");
        Delete.Table("pending_messages");
    }
}
